# tests/test_plc_link_policy.py
# -*- coding: utf-8 -*-
"""PLC 링크 정책 검증 — 네트워크 없이(가짜 클라이언트) + 루프백 실소켓 1건.

pytest 로도, `python tests/test_plc_link_policy.py` 로도 실행된다.
"""
from __future__ import annotations

import asyncio
import os
import socket
import struct
import sys
import threading
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import device.plc as PLC                                  # noqa: E402
_REAL_CLIENT = PLC.ModbusTcpClient                        # 실소켓 테스트에서 복원용
from device.plc import AsyncPLC, PLCError                 # noqa: E402
from lib import config_common as cfgc                     # noqa: E402
from lib import link_state                                # noqa: E402
from pymodbus.exceptions import ConnectionException, ModbusIOException   # noqa: E402
from pymodbus.pdu import ExceptionResponse                # noqa: E402
import pymodbus                                           # noqa: E402


def _pm_version() -> tuple:
    try:
        return tuple(int(x) for x in str(pymodbus.__version__).split(".")[:2])
    except Exception:
        return (0, 0)


_PM_VER = _pm_version()
_ORIG_PROBE_CONNECT = AsyncPLC.__dict__["_probe_connect"].__func__   # test_11 이 클래스 속성을 덮어쓰므로 원본 보관


# ───────────────────────── 가짜 클라이언트 ─────────────────────────
class _Resp:
    def __init__(self, bits=None, registers=None):
        self.bits = bits if bits is not None else [True]
        self.registers = registers if registers is not None else [0]

    def isError(self):
        return False


class _Tr:
    def __init__(self):
        self.max_until_disconnect = 5
        self.count_until_disconnect = 5


class FakeClient:
    """호출 스크립트로 각 I/O 결과를 지정하는 가짜 pymodbus 클라이언트."""
    instances: list["FakeClient"] = []
    script: list = []          # 각 I/O 호출의 결과: "ok" | Exception | ("delay", sec)
    connect_ok = True
    connect_calls = 0

    def __init__(self, host, port=502, timeout=2.0, **kw):
        self.host, self.port, self.timeout = host, port, timeout
        self.kw = dict(kw)
        self.connected = False
        self.closed = 0
        self.io_calls = 0
        self.socket = None
        self.transaction = _Tr()
        FakeClient.instances.append(self)

    # --- 연결 ---
    def connect(self):
        FakeClient.connect_calls += 1
        self.connected = bool(FakeClient.connect_ok)
        if self.connected:
            self.socket = _FakeSock()
        return self.connected

    def close(self):
        self.closed += 1
        self.connected = False
        self.socket = None

    def is_socket_open(self):
        return self.connected

    # --- I/O ---
    def _next(self):
        self.io_calls += 1
        if not FakeClient.script:
            return _Resp()
        item = FakeClient.script.pop(0)
        if isinstance(item, tuple) and item and item[0] == "delay":
            time.sleep(item[1])
            return _Resp()
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, _Resp) or isinstance(item, ExceptionResponse):
            return item
        return _Resp()

    def read_coils(self, address, count=1, slave=0, **kw):
        return self._next()

    def write_coil(self, address, value, slave=0, **kw):
        return self._next()

    def read_holding_registers(self, address, count=1, slave=0, **kw):
        return self._next()

    def write_register(self, address, value, slave=0, **kw):
        return self._next()


class _FakeSock:
    def setsockopt(self, *a): pass
    def setblocking(self, *a): pass
    def recv(self, n): raise BlockingIOError()


def _mk(**cfg):
    """가짜 클라이언트를 쓰는 AsyncPLC 인스턴스 + 로그 수집기."""
    FakeClient.instances = []
    FakeClient.script = []
    FakeClient.connect_ok = True
    FakeClient.connect_calls = 0
    PLC.ModbusTcpClient = FakeClient          # type: ignore[assignment]

    logs: list[str] = []

    def _log(fmt, *a):
        try:
            logs.append(fmt % a if a else str(fmt))
        except Exception:
            logs.append(str(fmt))

    p = AsyncPLC(ip="127.0.0.1", port=1502, unit=1, timeout_s=0.5, logger=_log)
    # ⚠ __init__ 이 _apply_cfg_from_config() 로 생성자 인자를 덮어쓰므로 여기서 다시 세운다
    p.cfg.ip = "127.0.0.1"
    p.cfg.port = 1502
    p.cfg.unit = 1
    p.cfg.timeout_s = 0.5
    p.cfg.connect_retry = 2
    p.cfg.connect_retry_delay_s = 0.0
    p.cfg.inter_cmd_gap_s = 0.0
    for k, v in cfg.items():
        setattr(p.cfg, k, v)
    p._logs = logs                            # type: ignore[attr-defined]
    return p, logs


def _cur(p):
    return p._client


def _set_cfg(**kw):
    for k, v in kw.items():
        setattr(cfgc, k, v)


def _reset_cfg():
    _set_cfg(PLC_TIMEOUT_CLOSE_AFTER=3, PLC_FG_CONNECT_ATTEMPTS=1,
             PLC_DIAG_PROBE=False, PLC_DIAG_MIN_INTERVAL_S=60.0,
             PLC_RECONNECT_BACKOFF_S=0.0)


# ───────────────────────── 1 ─────────────────────────
def test_1_single_timeout_keeps_socket():
    _reset_cfg()
    p, logs = _mk()
    FakeClient.script = [ModbusIOException("No response received"), _Resp([True])]
    v = asyncio.run(p.read_coil(10))
    assert v is True
    cli = _cur(p)
    assert cli.closed == 0, "타임아웃 1회로는 소켓을 닫지 않는다"
    assert FakeClient.connect_calls == 1, "재접속 없음"
    assert not any("소켓 재생성" in m for m in logs), logs
    assert p._consec_timeouts == 0, "성공하면 카운터 리셋"


# ───────────────────────── 2 ─────────────────────────
def test_2_consecutive_timeouts_recreate_socket():
    _reset_cfg()
    _set_cfg(PLC_TIMEOUT_CLOSE_AFTER=3)
    p, logs = _mk()
    TO = ModbusIOException

    # 1회차: 첫 시도 TO → 같은 소켓 재시도도 TO  (_consec=2)
    FakeClient.script = [TO("no resp"), TO("no resp")]
    try:
        asyncio.run(p.read_coil(10))
    except PLCError as e:
        assert e.code == "E402", e.code
    assert p._consec_timeouts == 2
    assert _cur(p).closed == 0, "아직 임계 미만 → 소켓 유지"

    # 2회차: 첫 시도 TO 로 임계(3) 도달 → close + 새 클라이언트 + 재시도 성공
    n_before = len(FakeClient.instances)
    FakeClient.script = [TO("no resp"), _Resp([True])]
    v = asyncio.run(p.read_coil(10))
    assert v is True
    assert len(FakeClient.instances) > n_before, "새 클라이언트 생성"
    rec = [m for m in logs if "소켓 재생성 #1" in m]
    assert rec, logs
    assert "연속 타임아웃" in rec[0], rec[0]


# ───────────────────────── 2b ─────────────────────────
def test_2b_new_socket_gets_fresh_budget():
    """임계로 소켓을 재생성한 직후 타임아웃 1회가 또 재생성을 부르면 안 된다.
    새 연결은 다시 PLC_TIMEOUT_CLOSE_AFTER 예산을 받는다."""
    _reset_cfg()
    _set_cfg(PLC_TIMEOUT_CLOSE_AFTER=3)
    p, logs = _mk()
    TO = ModbusIOException

    # 연속 3회로 임계 도달 → 재생성 (재시도는 성공시키지 않고 실패로 끝낸다)
    FakeClient.script = [TO("x"), TO("x")]                    # consec 2
    with __import__("contextlib").suppress(PLCError):
        asyncio.run(p.read_coil(1))
    FakeClient.script = [TO("x"), TO("x")]                    # consec 3 → 재생성 후 재시도도 TO
    with __import__("contextlib").suppress(PLCError):
        asyncio.run(p.read_coil(1))
    assert any("소켓 재생성" in m for m in logs), logs
    # ✅ 재접속에 성공했으므로 카운터는 0 이어야 한다(새 예산)
    assert p._consec_timeouts == 0, f"새 소켓은 새 예산: {p._consec_timeouts}"

    # 재생성 직후 타임아웃 1회 → 추가 close 없이 같은 소켓 재시도 후 성공
    cli = _cur(p)
    closed_before = cli.closed
    n_recon_before = len([m for m in logs if "소켓 재생성" in m])
    FakeClient.script = [TO("x"), _Resp([True])]
    v = asyncio.run(p.read_coil(1))
    assert v is True
    assert _cur(p) is cli, "같은 소켓을 유지해야 한다"
    assert cli.closed == closed_before, "추가 close 호출 0회"
    assert len([m for m in logs if "소켓 재생성" in m]) == n_recon_before, "추가 재생성 없음"
    assert p._consec_timeouts == 0, "성공하면 카운터 0"


# ───────────────────────── 3 ─────────────────────────
def test_3_connection_exception_reconnects():
    _reset_cfg()
    p, logs = _mk()
    FakeClient.script = [ConnectionException("peer closed"), _Resp([True])]
    v = asyncio.run(p.read_coil(10))
    assert v is True
    rec = [m for m in logs if "소켓 재생성" in m]
    assert rec, logs
    assert "E401" in rec[0], rec[0]
    assert FakeClient.connect_calls >= 2, "재접속했다"


# ───────────────────────── 4 ─────────────────────────
def test_4_low_priority_never_reconnects():
    _reset_cfg()
    p, logs = _mk()
    FakeClient.script = [ModbusIOException("no resp")]
    try:
        asyncio.run(p.read_coils_block(0, 4, priority="low"))
        raise AssertionError("E402 가 나야 한다")
    except PLCError as e:
        assert e.code == "E402", e.code
    assert _cur(p).closed == 0, "low 는 소켓을 닫지 않는다"
    assert FakeClient.connect_calls == 1, "low 는 재접속하지 않는다"
    assert _cur(p).io_calls == 1, "low 는 재시도하지 않는다"
    assert not any("소켓 재생성" in m for m in logs)


# ───────────────────────── 5 ─────────────────────────
def test_5_e403_no_retry():
    _reset_cfg()
    p, logs = _mk()
    FakeClient.script = [ExceptionResponse(1, 2)]
    try:
        asyncio.run(p.read_coil(10))
        raise AssertionError("E403 가 나야 한다")
    except PLCError as e:
        assert e.code == "E403", e.code
    assert _cur(p).io_calls == 1, "재시도 없음"
    assert _cur(p).closed == 0


# ───────────────────────── 6 ─────────────────────────
def test_6_connect_attempts_fg_vs_bg_and_backoff():
    _reset_cfg()
    _set_cfg(PLC_FG_CONNECT_ATTEMPTS=1)
    # 앞단(원시 연산): 1회
    p, _ = _mk()
    FakeClient.connect_ok = False
    try:
        asyncio.run(p.read_coil(1))
    except PLCError as e:
        assert e.code == "E401"
    assert FakeClient.connect_calls == 1, FakeClient.connect_calls

    # 명시적 connect(): connect_retry+1 = 3회
    p, _ = _mk()
    FakeClient.connect_ok = False
    try:
        asyncio.run(p.connect())
    except PLCError as e:
        assert e.code == "E401"
    assert FakeClient.connect_calls == 3, FakeClient.connect_calls

    # 백오프 중이면 시도 0회 + backoff 메시지
    _set_cfg(PLC_RECONNECT_BACKOFF_S=30.0)
    p, _ = _mk()
    p._client = None
    p._next_connect_attempt_at = time.monotonic() + 30.0
    FakeClient.connect_calls = 0
    try:
        asyncio.run(p.read_coil(1))
        raise AssertionError("E401(backoff) 가 나야 한다")
    except PLCError as e:
        assert e.code == "E401"
        assert "PLC 재연결 대기 중 (backoff)" in str(e), str(e)
    assert FakeClient.connect_calls == 0, "백오프 중엔 소켓을 건드리지 않는다"
    _reset_cfg()


# ───────────────────────── 7 ─────────────────────────
def test_7_cancel_keeps_lock_until_thread_done():
    _reset_cfg()
    p, _ = _mk()
    FakeClient.script = [("delay", 0.5)]

    async def _main():
        t = asyncio.ensure_future(asyncio.wait_for(p.read_coil(1), timeout=0.1))
        await asyncio.sleep(0.25)               # 취소는 이미 일어났다
        assert p._lock.locked(), "스레드가 도는 동안 락이 풀리면 안 된다"
        # 다른 태스크가 그 사이 락을 잡지 못하는지
        got = False
        try:
            await asyncio.wait_for(p._lock.acquire(), timeout=0.05)
            got = True
            p._lock.release()
        except asyncio.TimeoutError:
            pass
        assert not got, "다른 태스크가 락을 선점하면 안 된다"
        with __import__("contextlib").suppress(Exception):
            await t
        await asyncio.sleep(0.45)
        assert not p._lock.locked(), "스레드 종료 후에는 풀린다"

    asyncio.run(_main())


# ───────────────────────── 8 ─────────────────────────
def test_8_heartbeat_policy():
    _reset_cfg()
    # (a) 락이 잡혀 있고 미연결이면 _mark_conn_ok 를 부르지 않는다
    p, _ = _mk()
    marks = []
    p._mark_conn_ok = lambda: marks.append("ok")        # type: ignore[assignment]
    p._mark_conn_fail = lambda: marks.append("fail")    # type: ignore[assignment]
    p._client = None

    async def _hb_once():
        await p._lock.acquire()
        try:
            # _heartbeat_loop 의 판정만 그대로 재현
            if p._lock.locked():
                if p.is_connected():
                    p._mark_conn_ok()
        finally:
            p._lock.release()

    asyncio.run(_hb_once())
    assert marks == [], "미연결인데 '연결됨'으로 보면 안 된다"

    # (b) ping 타임아웃 1회로는 close 하지 않는다
    p, logs = _mk()
    asyncio.run(p.read_coil(1))                 # 소켓 확보
    cli = _cur(p)
    before = cli.closed
    FakeClient.script = [ModbusIOException("no resp"), _Resp([True])]

    async def _ping():
        async with p._io_lock("heartbeat", addr=0):
            await p._locked_thread(p._connect_sync, full=True)
            await p._throttle_and_heartbeat()
            await p._run_io("heartbeat", 0, "read_coils", 0, count=1)

    asyncio.run(_ping())
    assert _cur(p).closed == before, "ping 타임아웃 1회로 소켓을 닫지 않는다"


# ───────────────────────── 9 ─────────────────────────
def test_9_transaction_counter_reset():
    _reset_cfg()
    p, _ = _mk()
    asyncio.run(p.read_coil(1))
    tr = _cur(p).transaction
    tr.count_until_disconnect = 1
    asyncio.run(p.read_coil(1))
    assert tr.count_until_disconnect == tr.max_until_disconnect, "성공 시 원복"


# ───────────────────────── 10 ─────────────────────────
def test_10_detect_uid_kw_device_id():
    _reset_cfg()
    p, _ = _mk()

    class _C311:
        def read_coils(self, address, count=1, device_id=0): ...
    assert p._detect_uid_kw(_C311().read_coils) == "device_id"

    class _C36:
        def read_coils(self, address, count=1, slave=0): ...
    assert p._detect_uid_kw(_C36().read_coils) == "slave"

    p._uid_kw = "device_id"
    p.cfg.unit = 7
    assert p._uid_kwargs() == {"device_id": 7}
    # 메서드를 주면 그 signature 로 다시 판정
    assert p._uid_kwargs(_C36().read_coils) == {"slave": 7}


# ───────────────────────── 11 ─────────────────────────
def test_11_diag_runs_once_per_interval():
    _reset_cfg()
    _set_cfg(PLC_DIAG_PROBE=True, PLC_DIAG_MIN_INTERVAL_S=60.0)
    p, logs = _mk()
    AsyncPLC._probe_connect = staticmethod(lambda h, po, t: "SYN 무응답(타임아웃)")   # type: ignore
    AsyncPLC._probe_ping = staticmethod(lambda h: "무응답")                          # type: ignore
    FakeClient.connect_ok = False
    for _ in range(3):
        try:
            asyncio.run(p.read_coil(1))
        except PLCError:
            pass
    for _ in range(40):
        if any("PLC 진단" in m for m in logs):
            break
        time.sleep(0.05)
    diag = [m for m in logs if "PLC 진단" in m]
    assert len(diag) == 1, f"60초 내 1회만: {diag}"
    assert "connect=SYN 무응답" in diag[0], diag[0]
    # 접속 실패 메시지에 시도횟수/분류가 붙는지
    fails = [m for m in logs if "Modbus TCP 연결 실패" in m]
    _reset_cfg()


# ───────────────────────── 12 ─────────────────────────
def test_12_diag_is_not_stale():
    """E401 메시지의 '분류=' 는 이전 실패의 묵은 진단값을 쓰지 않는다."""
    _reset_cfg()
    _set_cfg(PLC_DIAG_PROBE=False, PLC_DIAG_MIN_INTERVAL_S=60.0)
    p, logs = _mk()
    FakeClient.connect_ok = False
    # (a) 묵은 값: 90초 전 기록
    p._last_connect_diag = "SYN 무응답(타임아웃, errno=10060)"
    p._last_connect_diag_ts = time.monotonic() - 90.0
    try:
        asyncio.run(p.read_coil(1))
        raise AssertionError("E401 이 나야 한다")
    except PLCError as e:
        assert e.code == "E401"
        assert "분류=미확인" in str(e), str(e)
        assert "SYN 무응답" not in str(e), str(e)
    # (b) 진단 스레드 진행 중
    p, logs = _mk()
    FakeClient.connect_ok = False
    p._last_connect_diag = "SYN 무응답(타임아웃, errno=10060)"
    p._last_connect_diag_ts = time.monotonic() - 90.0
    p._diag_running = True
    try:
        asyncio.run(p.read_coil(1))
    except PLCError as e:
        assert "분류=진단중" in str(e), str(e)
    # (c) 신선한 값은 그대로 쓴다
    p, logs = _mk()
    FakeClient.connect_ok = False
    p._last_connect_diag = "접속 거부(RST, errno=10061)"
    p._last_connect_diag_ts = time.monotonic()
    try:
        asyncio.run(p.read_coil(1))
    except PLCError as e:
        assert "분류=접속 거부(RST, errno=10061)" in str(e), str(e)
    _reset_cfg()


# ───────────────────────── 13 ─────────────────────────
def test_13_probe_connect_errno_map():
    class _Sk:
        rc = 0
        def __init__(self, *a, **k): pass
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def settimeout(self, t): pass
        def connect_ex(self, addr): return _Sk.rc

    orig = PLC.socket.socket
    PLC.socket.socket = _Sk          # type: ignore[assignment]
    try:
        expect = {
            0:     "접속됨",
            10035: "판정보류(접속 진행중, errno=10035)",
            10056: "접속됨(이미 연결, errno=10056)",
            10013: "차단됨(방화벽/권한, errno=10013)",
            10060: "SYN 무응답(타임아웃, errno=10060)",
            10061: "접속 거부(RST, errno=10061)",
            10065: "도달 불가(errno=10065)",
            12345: "미분류(errno=12345)",
        }
        for rc, want in expect.items():
            _Sk.rc = rc
            got = _ORIG_PROBE_CONNECT("10.0.0.1", 502, 0.5)
            assert got == want, (rc, got, want)
    finally:
        PLC.socket.socket = orig      # type: ignore[assignment]


# ───────────────────────── 14 ─────────────────────────
def test_14_link_state_toggled():
    """첫 실패에 link down, 복구에 up. 끊김 감지 1줄 / 복구 1줄 — 채팅 알림 횟수는 기존과 동일."""
    _reset_cfg()
    link_state.set_plc_link_down(False)
    link_state.consume_suppressed()
    # (a) 임계 미만 끊김: 알림 0회, 로그는 감지 1 + 복구 1
    p, logs = _mk()
    fired = []
    p._fire_conn_change = lambda ok, detail="": fired.append((ok, detail))   # type: ignore[assignment]
    p._disconnect_alert_after_s = 60.0
    assert link_state.is_plc_link_down() is False
    p._mark_conn_fail()
    assert link_state.is_plc_link_down() is True, "첫 실패에 link down"
    for _ in range(4):
        p._mark_conn_fail()
    link_state.note_suppressed("E401"); link_state.note_suppressed("E402")   # 끊긴 동안 억제된 카드 가정
    p._mark_conn_ok()
    assert link_state.is_plc_link_down() is False, "복구에 link up"
    down = [m for m in logs if m.startswith("WARN PLC 링크 끊김 감지")]
    up = [m for m in logs if m.startswith("WARN PLC 링크 복구")]
    assert len(down) == 1, down
    assert len(up) == 1 and "채팅알림=미발송" in up[0], up
    # 2026-09-23: 억제 카운터는 링크 up 전이(note_link_up → 재연결 카드)가 소진한다.
    #             _mark_conn_ok 의 복구 로그는 조회만 하므로 전이 후에는 0 이다.
    assert link_state.suppressed_total() == 0
    assert fired == [], fired
    # 복구 뒤에는 새 사이클
    p._mark_conn_fail()
    assert len([m for m in logs if m.startswith("WARN PLC 링크 끊김 감지")]) == 2

    # (b) 임계 초과 끊김: 끊김 알림 1 + 재연결 알림 1 (기존과 동일), 로그 감지 1 + 복구 1
    p, logs = _mk()
    fired = []
    p._fire_conn_change = lambda ok, detail="": fired.append((ok, detail))   # type: ignore[assignment]
    p._disconnect_alert_after_s = 0.0
    for _ in range(5):
        p._mark_conn_fail()
    p._mark_conn_ok()
    assert [ok for ok, _ in fired] == [False, True], fired
    assert link_state.is_plc_link_down() is False
    assert len([m for m in logs if m.startswith("WARN PLC 링크 끊김 감지")]) == 1
    up = [m for m in logs if m.startswith("WARN PLC 링크 복구")]
    assert len(up) == 1 and "채팅알림=발송" in up[0], up
    # 끊김 상태가 아니었으면 복구 로그도 없다
    p._mark_conn_ok()
    assert len([m for m in logs if m.startswith("WARN PLC 링크 복구")]) == 1


# ───────────────────────── 15~19: 저우선 격리 ─────────────────────────
def test_15_low_timeout_does_not_touch_high_counter():
    _reset_cfg()
    p, logs = _mk()
    asyncio.run(p.read_coil(1))                 # 소켓 확보 (high)
    cli = _cur(p)
    connects = FakeClient.connect_calls
    for _ in range(10):
        FakeClient.script = [ModbusIOException("no resp")]
        try:
            asyncio.run(p.read_coils_block(0, 4, priority="low"))
            raise AssertionError("E402 가 나야 한다")
        except PLCError as e:
            assert e.code == "E402", e.code
    assert p._consec_timeouts == 0, p._consec_timeouts
    assert p._consec_timeouts_low == 10, p._consec_timeouts_low
    assert _cur(p) is cli and cli.closed == 0, "소켓 재생성 0회"
    assert FakeClient.connect_calls == connects
    assert not any("소켓 재생성" in m for m in logs)


def test_16_low_e401_does_not_reconnect():
    _reset_cfg()
    p, logs = _mk()
    asyncio.run(p.read_coil(1))                 # 소켓 확보
    cli = _cur(p)
    connects = FakeClient.connect_calls
    FakeClient.script = [ConnectionException("Connection unexpectedly closed")]
    try:
        asyncio.run(p.read_coils_block(0, 4, priority="low"))
        raise AssertionError("E401 이 나야 한다")
    except PLCError as e:
        assert e.code == "E401", e.code
    assert _cur(p) is cli and cli.closed == 0, "low 는 close 하지 않는다"
    assert FakeClient.connect_calls == connects, "low 는 connect 하지 않는다"
    assert cli.io_calls == 2, "low 는 재시도하지 않는다 (확보 1 + 실패 1)"
    assert p._consec_timeouts == 0 and p._consec_timeouts_low == 1
    assert not any("소켓 재생성" in m for m in logs)


def test_17_low_success_does_not_clear_high_counter():
    _reset_cfg()
    _set_cfg(PLC_TIMEOUT_CLOSE_AFTER=10)
    p, logs = _mk()
    asyncio.run(p.read_coil(1))                 # 소켓 확보
    # high 2회 타임아웃(각 호출: 실패 → 같은 소켓 재시도도 실패 → +1)
    for _ in range(2):
        FakeClient.script = [ModbusIOException("no resp"), _Resp([True])]
        asyncio.run(p.read_coil(1))
    # 첫 실패는 +1, 재시도 성공은 카운터를 0 으로 되돌리므로 직접 2 로 세팅해 시나리오를 고정
    p._consec_timeouts = 2
    p._consec_timeouts_low = 3
    FakeClient.script = [_Resp([True, False, True, False])]
    asyncio.run(p.read_coils_block(0, 4, priority="low"))
    assert p._consec_timeouts == 2, "low 성공은 high 카운터를 지우지 않는다"
    assert p._consec_timeouts_low == 0
    # high 성공은 둘 다 지운다
    p._consec_timeouts_low = 3
    asyncio.run(p.read_coil(1))
    assert p._consec_timeouts == 0 and p._consec_timeouts_low == 0
    _reset_cfg()


def test_18_low_does_not_connect_when_down():
    _reset_cfg()
    p, logs = _mk()
    asyncio.run(p.read_coil(1))                 # 소켓 확보
    # 링크 다운: 소켓이 닫혔다(재접속 실패 후의 상태)
    asyncio.run(p._locked_thread(p._close_sync))
    assert not p.is_connected() and p._disconnected_at > 0.0
    connects = FakeClient.connect_calls
    try:
        asyncio.run(p.read_coils_block(0, 4, priority="low"))
        raise AssertionError("E401 이 나야 한다")
    except PLCError as e:
        assert e.code == "E401" and "저우선" in str(e), str(e)
    assert FakeClient.connect_calls == connects, "low 는 접속을 시도하지 않는다"
    assert p._consec_timeouts == 0


def test_19_logger_self_backoff_by_low_counter():
    _reset_cfg()
    _set_cfg(PLC_COIL_LOG_BACKOFF_AFTER=2)
    p, logs = _mk()
    asyncio.run(p.read_coil(1))                 # 연결 상태
    p._consec_timeouts = 0
    p._consec_timeouts_low = 2
    snaps = []

    async def _snap(keys=None):
        snaps.append(1)
        return {}
    p.snapshot_all_coils_fast = _snap           # type: ignore[assignment]
    p._plc_coil_log_stop = asyncio.Event()
    p._plc_coil_log_interval = 0.01
    p._plc_coil_log_keys = list(PLC.PLC_COIL_MAP.keys())[:4]
    p._plc_reg_log_keys = []
    _set_cfg(PLC_COIL_LOG_SUMMARY_S=0.0)        # 요약 주기 하한(1초)으로 → 1초 뒤 요약 1회
    plan_results = []
    p._note_coil_plan_result = lambda addrs, ok: plan_results.append(ok)   # type: ignore[assignment]

    async def _run():
        t = asyncio.create_task(p._plc_coil_log_loop())
        await asyncio.sleep(1.15)
        p._plc_coil_log_stop.set()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(t, timeout=1.0)
    import contextlib
    asyncio.run(_run())
    assert snaps == [], "low 임계 도달 시 스냅샷을 시도하지 않는다"
    assert plan_results and all(ok is False for ok in plan_results), "backoff 경로로 집계"
    summ = [m for m in logs if "PLC COIL LOG summary" in m]
    # low_fail 은 구간 누적 카운터(이 구간엔 스냅샷 시도 자체가 없었으므로 0), 억제는 backoff 로 집계된다
    assert summ and "low_fail=0" in summ[0] and "backoff=" in summ[0], summ
    assert "backoff=0 " not in summ[0], summ[0]
    _reset_cfg()


# ───────────────────────── 실소켓 ─────────────────────────
class _MiniModbusServer(threading.Thread):
    """FC1 만 흉내내는 최소 Modbus/TCP 서버. mode: ok|delay|silent|close"""

    def __init__(self):
        super().__init__(daemon=True)
        self.last_reply_ts = 0.0   # 마지막 응답 시각(유휴 판정용)
        self.srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("127.0.0.1", 0))
        self.srv.listen(4)
        self.port = self.srv.getsockname()[1]
        self.mode = "ok"
        self.accepts = 0
        self._stop = False

    def run(self):
        while not self._stop:
            try:
                self.srv.settimeout(0.2)
                try:
                    c, _ = self.srv.accept()
                except socket.timeout:
                    continue
                self.accepts += 1
                threading.Thread(target=self._serve, args=(c,), daemon=True).start()
            except Exception:
                break

    @staticmethod
    def _recv_exact(c, n):
        """n 바이트를 다 받을 때까지 읽는다. 부족하면 None."""
        buf = b""
        while len(buf) < n:
            try:
                b = c.recv(n - len(buf))
            except Exception:
                return None
            if not b:
                return None
            buf += b
        return buf

    def _serve(self, c):
        c.settimeout(5.0)
        try:
            while not self._stop:
                hdr = self._recv_exact(c, 7)     # MBAP: tid(2) pid(2) len(2) uid(1)
                if hdr is None:
                    return
                tid, _pid, ln, _uid = struct.unpack(">HHHB", hdr)
                # ⚠ length 필드는 unit id 를 '포함' 한다. 남은 PDU 는 ln-1 바이트.
                #    (FC1 요청은 PDU 5바이트라 프레임 전체가 12바이트다.
                #     예전처럼 11바이트만 읽으면, 소켓을 유지하는 pymodbus 3.11 에서
                #     두 번째 요청부터 헤더가 1바이트 밀려 transaction id 가 어긋난다)
                need = max(0, int(ln) - 1)
                if need and self._recv_exact(c, need) is None:
                    return

                m = self.mode
                if m == "close":
                    c.close()
                    return
                if m == "silent":
                    continue           # 응답 안 함
                if m == "delay":
                    time.sleep(1.2)    # timeout(0.5s) 초과 → 늦은 응답
                body = struct.pack(">BBB", 1, 1, 0x01)     # FC1, bytecount 1, bits=0x01
                c.sendall(struct.pack(">HHHB", tid, 0, len(body) + 1, 1) + body)
                self.last_reply_ts = time.monotonic()
        finally:
            with __import__("contextlib").suppress(Exception):
                c.close()

    def wait_idle(self, quiet_s: float = 0.1, timeout_s: float = 3.0) -> None:
        """서버가 마지막 응답을 보낸 뒤 quiet_s 동안 조용해질 때까지 기다린다.
        단일 스레드 서버가 delay sleep 중인데 다음 요청을 보내 또 타임아웃나는 것을 막는다."""
        end = time.monotonic() + timeout_s
        while time.monotonic() < end:
            last = self.last_reply_ts
            if last and (time.monotonic() - last) >= quiet_s:
                return
            if not last:
                time.sleep(quiet_s)
                return
            time.sleep(0.02)

    def stop(self):
        self._stop = True
        with __import__("contextlib").suppress(Exception):
            self.srv.close()


def test_z_real_socket_policy():
    """진짜 pymodbus 로 루프백 서버에 붙어 정책을 확인한다.
    (reload 하면 PLCError 클래스가 바뀌어 다른 테스트의 except 가 깨지므로 복원만 한다)"""
    PLC.ModbusTcpClient = _REAL_CLIENT
    RealPLC = AsyncPLC

    _reset_cfg()
    _set_cfg(PLC_TIMEOUT_CLOSE_AFTER=3)
    srv = _MiniModbusServer()
    srv.start()
    time.sleep(0.1)
    logs: list[str] = []
    p = RealPLC(ip="127.0.0.1", port=srv.port, unit=1, timeout_s=0.5,
                logger=lambda f, *a: logs.append(f % a if a else str(f)))
    p.cfg.ip = "127.0.0.1"
    p.cfg.port = srv.port
    p.cfg.unit = 1
    p.cfg.timeout_s = 0.5
    p.cfg.connect_retry = 0
    p.cfg.connect_retry_delay_s = 0.0
    p.cfg.inter_cmd_gap_s = 0.0
    ours: list[int] = []
    _orig_close = p._close_sync
    p._close_sync = lambda: (ours.append(1), _orig_close())[1]   # type: ignore[assignment]

    try:
        # (a) 정상 1회 → accept 1
        asyncio.run(p.read_coil(0))
        assert srv.accepts == 1, srv.accepts

        # (a') 응답 지연 → 타임아웃
        #  ⚠ 버전별 동작이 다르다.
        #    - pymodbus 3.6.x : ModbusTransactionManager.execute 가 응답이 없으면
        #      self.client.close() 를 스스로 호출한다 → 소켓 재생성을 우리가 막을 수 없다.
        #    - pymodbus 3.11.x (requirements.build.txt 고정) : 동기 클라이언트는
        #      타임아웃에 소켓을 닫지 않고 "No response received after N retries,
        #      continue with next request" 만 남기고 같은 소켓을 계속 쓴다.
        #      (장비 로그에 찍힌 문구가 이 3.11.2 것이다)
        #    어느 쪽이든 우리 계층은 close 하지 않고 E402 로 분류해 정책을 태운다.
        srv.mode = "delay"
        code = ""
        try:
            asyncio.run(p.read_coil(0))
        except PLCError as e:
            code = e.code
        assert code == "E402", f"타임아웃은 E402 여야 한다(E403 아님): {code}"
        assert ours == [], "우리 계층은 타임아웃으로 소켓을 닫지 않는다"
        assert not any("소켓 재생성" in m for m in logs),             [m for m in logs if "재생성" in m]
        assert p._consec_timeouts >= 1, "연속 타임아웃이 계수돼야 한다"

        if _PM_VER >= (3, 7):
            assert srv.accepts == 1, f"pymodbus>=3.7 은 타임아웃에 소켓을 닫지 않는다 (accepts={srv.accepts})"

        # 단일 스레드 서버가 아직 delay sleep 중일 수 있다 → 유휴가 될 때까지 대기
        srv.mode = "ok"
        srv.wait_idle()
        asyncio.run(p.read_coil(0))
        assert p._consec_timeouts == 0, "성공하면 리셋"

        # (b) 서버가 끊으면 재접속한다
        base = srv.accepts
        srv.mode = "close"
        with __import__("contextlib").suppress(Exception):
            asyncio.run(p.read_coil(0))
        srv.mode = "ok"
        with __import__("contextlib").suppress(Exception):
            asyncio.run(p.read_coil(0))
        assert srv.accepts > base, f"끊기면 재접속해야 한다 (accepts={srv.accepts})"

        # (c) 연속 silent
        #  ⚠ 버전에 따라 결과가 다르다.
        #    - 3.11.x: 소켓이 유지되므로 연속 타임아웃이 누적돼 임계에 도달하고,
        #      우리 계층이 "소켓 재생성 ... 연속 타임아웃" 로그를 남긴다.
        #    - 3.6.x : 라이브러리가 매번 소켓을 닫아 _connect_sync 가 재접속하고,
        #      그때마다 '새 소켓은 새 예산' 정책으로 카운터가 0 으로 리셋된다.
        #      따라서 우리 임계에는 도달하지 않는다(우리가 닫는 게 아니다).
        #    임계 경로 자체는 가짜 클라이언트 테스트(test_2 / test_2b)가 결정적으로 덮는다.
        ours.clear()
        logs.clear()
        p._consec_timeouts = 0
        srv.mode = "silent"
        for _ in range(4):
            with __import__("contextlib").suppress(Exception):
                asyncio.run(p.read_coil(0))
        if _PM_VER >= (3, 7):
            assert any("소켓 재생성" in m and "연속 타임아웃" in m for m in logs), [m for m in logs if "재생성" in m]
        else:
            assert ours == [], "3.6.x 에서는 라이브러리가 닫으므로 우리는 닫지 않는다"
    finally:
        with __import__("contextlib").suppress(Exception):
            asyncio.run(p.close())
        srv.stop()


def _run_all():
    fns = [(n, f) for n, f in sorted(globals().items())
           if n.startswith("test_") and callable(f)]
    fails = []
    for n, f in fns:
        try:
            f()
            print(f"  OK   {n}")
        except Exception as e:
            import traceback
            fails.append(n)
            print(f"  FAIL {n}: {type(e).__name__}: {e}")
            traceback.print_exc()
    print("=" * 56)
    print(f"실패 {len(fails)}건" + (": " + ", ".join(fails) if fails else " — 전부 통과"))
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(_run_all())

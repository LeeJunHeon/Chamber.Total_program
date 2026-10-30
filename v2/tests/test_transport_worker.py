"""전송 계층과 worker 시험 — 실제 COM 포트를 열지 않는다(가짜 serial 객체·가짜 장비)."""
import threading
import time

import pytest

from v2.common.clock import RealClock, VirtualClock
from v2.common.simdev import LineDevice, line_read_many, line_write
from v2.common.transport import (
    Exchange, PortGuard, ReadTimeout, RecordingTransport, ReplayMismatch, ReplayTransport, SerialSettings,
    SerialTransport, SimTransport, TransportError,
)
from v2.common.worker import Request, Sequence, Status, Worker


# ───────────── 도우미 ─────────────
class Box:
    def __init__(self, clock):
        self.clock, self.items = clock, []

    def cb(self, x):
        with self.clock.cond:
            self.items.append(x)
            self.clock.cond.notify_all()

    def wait(self, n, until=None):
        return self.clock.wait_until(until, lambda: len(self.items) >= n)


def make(dev, **kw):
    c = VirtualClock()
    c.register()
    ev = Box(c)
    w = Worker("dev", SimTransport(c, dev), on_event=ev.cb, **kw)
    w.start()
    return c, w, ev


def read_req(c, box, deadline=0.9, names=("a",)):
    return Request("read", lambda s: line_read_many(s, "d", list(names)), deadline=c.monotonic() + deadline,
                   on_done=box.cb, name="read")


# ───────────── worker ─────────────
def test_read_ok_latency():
    dev = LineDevice("d", {"a": 1.5, "b": 2}, latency_s=0.05)
    c, w, _ = make(dev)
    box = Box(c)
    w.submit(read_req(c, box, names=("a", "b")))
    box.wait(1)
    r = box.items[0]
    assert r.status == Status.OK and r.value == {"d.a": 1.5, "d.b": 2.0}
    assert abs((r.finished - r.started) - 0.05) < 1e-9 and w.last_ok == r.finished
    w.stop()


def test_timeouts_then_reconnect():
    dev = LineDevice("d", {"a": 1}, latency_s=0.05)
    dev.add_fault("no_response", 0.0, 2.5)
    c, w, ev = make(dev, fail_streak=3)
    box = Box(c)
    for i in range(3):
        w.submit(read_req(c, box))
        box.wait(i + 1)
    assert [r.status for r in box.items] == [Status.TIMEOUT] * 3
    assert box.items[0].finished == pytest.approx(0.9)
    c.wait_until(10.0)
    kinds = [(e.kind, round(e.t, 3)) for e in ev.items]
    assert ("disconnected", 2.7) in kinds          # 세 번째 무응답(1.8~2.7) 뒤 끊음
    assert ("connected", 3.7) in kinds             # 백오프 1 s 뒤 재연결
    w.submit(read_req(c, box))
    box.wait(4)
    assert box.items[3].status == Status.OK
    w.stop()


def test_disconnect_window_backoff_and_long_outage():
    dev = LineDevice("d", {"a": 1})
    dev.add_fault("disconnect", 0.0, 10.0)
    c, w, ev = make(dev, long_outage_s=5.0, probe_s=60.0)
    box = Box(c)
    w.submit(read_req(c, box))
    box.wait(1)
    assert box.items[0].status == Status.LINK_DOWN and box.items[0].reason == "LINK_DOWN"
    c.wait_until(80.0)
    fails = [round(e.t, 3) for e in ev.items if e.kind == "connect_failed"]
    assert fails[:4] == [0.0, 1.0, 3.0, 7.0]       # 1·2·4 s 지수 백오프
    assert [e.kind for e in ev.items].count("long_outage") == 1   # 장기 두절 알림 1회
    conn = [round(e.t, 3) for e in ev.items if e.kind == "connected"]
    assert conn == [67.0]                          # 7 s 에 장기 두절 → 60 s 프로브
    w.stop()


def test_safety_preempts_and_purges():
    dev = LineDevice("d", {"a": 1, "stop": 0}, latency_s=0.8)
    c, w, _ = make(dev)
    box = Box(c)
    w.submit(read_req(c, box, deadline=0.9))                     # 진행 중이 될 읽기
    c.wait_until(0.05)
    w.submit(read_req(c, box, deadline=1.9))                     # 큐의 읽기
    w.submit(Request("write", lambda s: line_write(s, "x", 1), deadline=5.0, on_done=box.cb, name="write"))
    c.wait_until(0.1)
    w.submit(Request("safety", lambda s: line_write(s, "stop", 1), deadline=5.0, on_done=box.cb,
                     name="safety", purge=True))
    box.wait(4)
    by = {(r.request.name, r.status) for r in box.items}
    assert ("read", Status.CANCELLED) in by and ("write", Status.CANCELLED) in by
    cancelled_inflight = [r for r in box.items if r.request.name == "read" and r.started is not None]
    assert cancelled_inflight and cancelled_inflight[0].finished == pytest.approx(0.1)
    safety = [r for r in box.items if r.request.name == "safety"][0]
    assert safety.status == Status.OK and safety.started == pytest.approx(0.1)
    assert dev.values["stop"] == "1"
    w.stop()


def test_priority_order():
    dev = LineDevice("d", {"a": 1}, latency_s=0.3)
    c, w, _ = make(dev)
    box, order = Box(c), []

    def rec(name):
        def fn(s):
            order.append(name)
            return line_read_many(s, "d", ["a"])
        return fn

    w.submit(Request("read", rec("first"), deadline=5.0, on_done=box.cb))
    c.wait_until(0.01)
    w.submit(Request("read", rec("read"), deadline=5.0, on_done=box.cb))
    w.submit(Request("write", rec("write"), deadline=5.0, on_done=box.cb))
    w.submit(Request("safety", rec("safety"), deadline=5.0, on_done=box.cb))
    box.wait(4)
    assert order == ["first", "safety", "write", "read"]
    w.stop()


def test_bench_rejects_writes():
    dev = LineDevice("d", {"a": 1})
    c, w, _ = make(dev, writes_allowed=False)
    box = Box(c)
    w.submit(Request("write", lambda s: line_write(s, "a", 2), deadline=5.0, on_done=box.cb))
    w.submit(Request("safety", lambda s: line_write(s, "a", 2), deadline=5.0, on_done=box.cb, purge=True))
    w.submit(read_req(c, box))
    box.wait(3)
    st = [(r.request.kind, r.status, r.reason) for r in box.items]
    assert ("write", Status.REJECTED, "BENCH_READ_ONLY") in st and ("safety", Status.REJECTED, "BENCH_READ_ONLY") in st
    assert ("read", Status.OK, None) in st and dev.writes == []
    seq_box = Box(c)
    w.submit_sequence(Sequence("s", [("x", lambda s: None)], on_done=lambda n, r, res: seq_box.cb(r)))
    assert seq_box.items == ["BENCH_READ_ONLY"]
    w.stop()


def test_sequence_steps_spread_with_reads_between():
    dev = LineDevice("d", {"a": 1}, latency_s=0.05)
    c, w, _ = make(dev)
    starts, box, done = [], Box(c), Box(c)

    def step(k):
        def fn(s):
            starts.append((k, round(c.monotonic(), 3)))
            line_write(s, k, 1)
        return fn

    w.submit_sequence(Sequence("start", [("s1", step("s1")), ("s2", step("s2")), ("s3", step("s3"))], gap_s=0.5,
                               on_done=lambda n, r, res: done.cb(r)))
    c.wait_until(0.2)
    w.submit(read_req(c, box))
    done.wait(1)
    box.wait(1)
    assert done.items == [None]
    assert starts == [("s1", 0.0), ("s2", 0.55), ("s3", 1.1)]
    assert box.items[0].started == pytest.approx(0.2)          # 시퀀스 사이에 읽기가 끼어듦
    w.stop()


def test_sequence_stops_on_failure():
    dev = LineDevice("d", {"a": 1}, latency_s=0.05)
    c, w, _ = make(dev)
    done = Box(c)

    def bad(s):
        raise ValueError("장비 거부")

    w.submit_sequence(Sequence("s", [("ok", lambda s: line_write(s, "a", 1)), ("bad", bad), ("never", lambda s: 1/0)],
                               on_done=lambda n, r, res: done.cb((r, len(res)))))
    done.wait(1)
    assert done.items == [("DEVICE_REJECTED", 2)]
    w.stop()


def test_expired_and_driver_error_and_suppress():
    dev = LineDevice("d", {"a": 1}, latency_s=0.05)
    dev.add_fault("garbage", 0.0, 1.0)
    c, w, ev = make(dev, fail_streak=2)
    box = Box(c)
    w.submit(Request("read", lambda s: 1, deadline=-1.0, on_done=box.cb))
    w.submit(read_req(c, box))
    box.wait(2)
    assert box.items[0].status == Status.EXPIRED
    assert box.items[1].status == Status.FAIL and box.items[1].reason == "DEVICE_REJECTED" and w.connected
    dev.add_fault("no_response", 1.0, 50.0)
    c.wait_until(1.0)
    w.suppress(until=40.0, reason="필라멘트 전환")
    for i in range(5):
        w.submit(read_req(c, box))
        box.wait(3 + i)
    assert all(r.status == Status.TIMEOUT for r in box.items[2:]) and w.connected   # 억제 중에는 세지 않음
    assert not [e for e in ev.items if e.kind == "disconnected"]
    w.stop()


# ───────────── 녹화·재생 ─────────────
def test_record_and_replay():
    c = VirtualClock()
    c.register()
    dev = LineDevice("d", {"a": 3}, latency_s=0.12)
    rec = RecordingTransport(SimTransport(c, dev))
    rec.open()
    rec.write(b"R a\r")
    assert rec.read_until(c.monotonic() + 1, b"\r") == b"a=3\r"
    rec.write(b"R zz\r")
    rec.read_until(c.monotonic() + 1, b"\r")
    rec.close()
    assert rec.exchanges[0] == Exchange(b"R a\r", b"a=3\r", pytest.approx(0.12))
    rp = ReplayTransport(c, rec.exchanges)
    rp.open()
    t0 = c.monotonic()
    rp.write(b"R a\r")
    assert rp.read_until(t0 + 1, b"\r") == b"a=3\r" and c.monotonic() - t0 == pytest.approx(0.12)
    with pytest.raises(ReplayMismatch):
        rp.write(b"R b\r")


# ───────────── pyserial 경로(가짜 serial 객체) ─────────────
class FakeSerial:
    def __init__(self, reply=None):
        self.buf, self.reply, self.closed, self.written = b"", reply or (lambda d: b""), False, []

    @property
    def in_waiting(self):
        return len(self.buf)

    def read(self, n):
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def write(self, d):
        self.written.append(d)
        self.buf += self.reply(d)

    def reset_input_buffer(self):
        self.buf = b""

    def close(self):
        self.closed = True


def _st(fake, **kw):
    return SerialTransport(RealClock(), SerialSettings("COM99", 9600), serial_factory=lambda s: fake, **kw)


def test_serial_read_until_and_leftover():
    fake = FakeSerial(lambda d: b"A=1\rB=2\r")
    t = _st(fake)
    t.open()
    t.write(b"R\r")
    dl = time.monotonic() + 0.5
    assert t.read_until(dl, b"\r") == b"A=1\r"
    assert t.read_until(dl, b"\r") == b"B=2\r"           # 남은 바이트는 다음 읽기에
    t.write(b"X")
    fake.buf = b"\x06\x01\x02"
    assert t.read_until(dl, size=3) == b"\x06\x01\x02"


def test_serial_timeout_partial():
    fake = FakeSerial(lambda d: b"A=")
    t = _st(fake)
    t.open()
    t.write(b"R\r")
    t0 = time.monotonic()
    with pytest.raises(ReadTimeout) as e:
        t.read_until(t0 + 0.05, b"\r")
    assert e.value.partial == b"A=" and time.monotonic() - t0 < 0.2


def test_serial_errors_become_transport_error():
    class Broken(FakeSerial):
        def write(self, d):
            raise OSError("장치 사라짐")
    t = _st(Broken())
    t.open()
    with pytest.raises(TransportError):
        t.write(b"x")
    t2 = SerialTransport(RealClock(), SerialSettings("COM99", 9600),
                         serial_factory=lambda s: (_ for _ in ()).throw(OSError("access denied")))
    with pytest.raises(TransportError):
        t2.open()


def test_port_guard_open_timeout_and_late_close():
    gate = threading.Event()
    late = FakeSerial()

    def slow(_s):
        gate.wait(5)
        return late

    guard = PortGuard()
    t = SerialTransport(RealClock(), SerialSettings("COM99", 9600), open_timeout_s=0.05, serial_factory=slow,
                        guard=guard)
    t0 = time.monotonic()
    with pytest.raises(TransportError):
        t.open()
    assert time.monotonic() - t0 < 0.5 and guard.stuck == 1
    with pytest.raises(TransportError):          # 막힌 열기가 끝나기 전에는 새로 열지 않는다
        t.open()
    gate.set()
    for _ in range(100):
        if guard.stuck == 0:
            break
        time.sleep(0.01)
    assert guard.stuck == 0 and late.closed       # 늦게 열린 객체는 스스로 닫힘


def test_connect_init_timeout():
    """연결 직후 초기화가 제한 시간 안에 응답이 없으면 0.3 s 뒤 1회 다시(받은 바이트 0), 그래도 없으면 끊고 백오프."""
    dev = LineDevice("d", {"a": 1}, latency_s=0.05)
    dev.add_fault("no_response", 0.0, 100.0)
    c = VirtualClock()
    c.register()
    ev = Box(c)
    w = Worker("dev", SimTransport(c, dev), on_event=ev.cb, connect_init_timeout_s=2.0,
               on_connect=lambda s: line_read_many(s, "d", ["a"]))
    w.start()
    c.wait_until(5.8)
    kinds = [(e.kind, round(e.t, 3)) for e in ev.items]
    assert kinds[:4] == [("connected", 0.0), ("connect_retry", 2.0), ("disconnected", 4.3), ("connected", 5.3)], kinds
    w.stop()


def test_write_without_reply_does_not_confirm_link():
    from v2.common.worker import SEQ_DONE
    dev = LineDevice("d", {"a": 1}, latency_s=0.05)
    c, w, _ = make(dev)
    box = Box(c)
    w.submit(read_req(c, box))
    box.wait(1)
    t_ok = w.last_ok
    c.wait_until(c.monotonic() + 1.0)
    w.submit(Request("write", lambda s: s.write(b"W a=2\r"), deadline=c.monotonic() + 1, on_done=box.cb,
                     confirms_link=False))
    box.wait(2)
    assert box.items[1].status == Status.OK and w.last_ok == t_ok      # 응답 없는 쓰기는 정상으로 세지 않음
    done = Box(c)
    w.submit_sequence(Sequence("early", [("a", lambda s: SEQ_DONE), ("never", lambda s: 1 / 0)],
                               on_done=lambda n, r, res: done.cb((r, len(res)))))
    done.wait(1)
    assert done.items == [(None, 1)]                                    # 일찍 성공으로 끝남
    w.stop()

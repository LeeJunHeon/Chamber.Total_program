"""시계·tick 스케줄러·ClockQueue 시험 — 가상 시계는 실제 시간을 기다리지 않는다."""
import threading

import pytest

from v2.common.clock import ClockDeadlock, ClockQueue, RealClock, TickScheduler, VirtualClock


def test_ticks_on_whole_seconds():
    c = VirtualClock(start_wall=1_760_000_000.37)
    c.register()
    s = TickScheduler(c)
    ts = [s.next_tick() for _ in range(5)]
    assert [t.t_wall for t in ts] == [1_760_000_001.0 + i for i in range(5)]
    assert all(t.jitter_ms == 0 and t.skipped == 0 for t in ts)
    assert [t.seq for t in ts] == [0, 1, 2, 3, 4]


def test_processing_time_does_not_accumulate():
    """매 tick 처리에 0.7 s 가 걸려도 다음 tick 은 정각 — 상대 sleep 누적이 없다."""
    c = VirtualClock(start_wall=100.0)
    c.register()
    s = TickScheduler(c)
    walls = []
    for _ in range(10):
        t = s.next_tick()
        walls.append(t.t_wall)
        c.advance(0.7)
    assert walls == [101.0 + i for i in range(10)]


def test_missed_ticks_counted():
    c = VirtualClock(start_wall=100.0)
    c.register()
    s = TickScheduler(c)
    s.next_tick()               # 101
    c.advance(3.4)              # 처리가 104.4 까지 밀림 → 102·103 놓침, 104 를 늦게
    t = s.next_tick()
    assert t.seq == 3 and t.skipped == 2 and t.t_wall == 104.0
    assert abs(t.jitter_ms - 400.0) < 1e-6
    t = s.next_tick()
    assert t.seq == 4 and t.skipped == 0 and t.jitter_ms == 0


def test_late_but_within_period_not_skipped():
    c = VirtualClock(start_wall=100.0)
    c.register()
    s = TickScheduler(c)
    s.next_tick()
    c.advance(1.5)              # 102.5 — tick 102 를 0.5 s 늦게
    t = s.next_tick()
    assert t.seq == 1 and t.skipped == 0 and abs(t.jitter_ms - 500) < 1e-6


def test_clock_queue_priority_and_not_before():
    c = VirtualClock()
    c.register()
    q = ClockQueue(c)
    q.put(2, "read")
    q.put(1, "write-later", not_before=5.0)
    q.put(0, "safety")
    assert q.get(until=0.0) == "safety"
    assert q.get(until=0.0) == "read"          # write-later 는 아직 not_before 전
    assert q.get(until=10.0) == "write-later"  # 가상 시각이 5.0 으로 이동
    assert c.monotonic() == 5.0
    assert q.get(until=12.0) is None and c.monotonic() == 12.0


def test_virtual_threads_advance_together():
    """두 스레드가 각자 기다릴 때 시각은 가장 이른 기한부터 옮겨진다."""
    c = VirtualClock()
    order = []
    lock = threading.Lock()

    def worker(name, wake):
        c.wait_until(wake)
        with lock:
            order.append((name, c.monotonic()))

    with c.hold():
        ts = [c.start_thread(lambda n=n, w=w: worker(n, w), n) for n, w in (("a", 3.0), ("b", 1.0))]
    for t in ts:
        t.join(5)
    assert order == [("b", 1.0), ("a", 3.0)]


def test_deadlock_detected():
    c = VirtualClock(deadlock_real_sec=0.5)
    c.register()
    with pytest.raises(ClockDeadlock):
        c.wait_until(None, lambda: False)


def test_real_clock_ticks_short_period():
    """실제 시계 + TickScheduler — 정각 격자(anchor + seq × 주기)에서, 일찍 깨지 않고, 늦으면 그 늦음이 jitter·skipped 로 정확히
    나온다(그 계산 자체는 위 가상 시계 시험들). 늦는 정도는 부하(다른 프로세스·스레드 스케줄링)에 달려 상한을 두지 않는다 —
    대신 jitter 가 돌아온 뒤 읽은 실제 시각의 늦음을 넘지 않는지(한쪽만 — 그 사이 선점돼도 참)와 주기 한 번 이상 늦은 tick 은
    건너뛰어 jitter 가 늘 주기보다 작은지를 본다."""
    c = RealClock()
    s = TickScheduler(c, period=0.05)
    out = []
    for _ in range(10):
        t = s.next_tick()
        out.append((t, c.wall()))
    ts = [t for t, _w in out]
    assert len(ts) == 10 and ts[0].seq == ts[0].skipped
    assert all(b.seq == a.seq + 1 + b.skipped for a, b in zip(ts, ts[1:]))                # 건너뛴 만큼만 seq 가 뛴다
    assert all(abs(t.t_wall - (s.anchor + t.seq * 0.05)) < 1e-6 for t in ts)                 # 정각 격자(밀림 없음)
    assert all(-2.0 <= t.jitter_ms < 50.0 for t in ts)                                       # 일찍 깨지 않음·늦음 < 주기
    assert all(t.jitter_ms <= (w - t.t_wall) * 1000.0 + 1.0 for t, w in out)               # jitter = 깨어난 시각 − 정각(그 뒤 읽기 ≥)


def test_anchor_fixed_at_first_tick_not_construction():
    c = VirtualClock(start_wall=100.0)
    c.register()
    s = TickScheduler(c)
    c.advance(2.7)                                  # 만든 뒤 준비 시간
    t = s.next_tick()
    assert t.t_wall == 103.0 and t.jitter_ms == 0 and t.skipped == 0 and t.seq == 0


def test_close_ends_waiting_threads_quietly():
    """5-4 ⑤ — 시험이 worker 를 멈추지 못하고 끝나도 시계를 닫으면 남은 스레드가 조용히 끝난다(ClockDeadlock 경고 없음)."""
    import threading
    from v2.common.clock import ClockClosed
    c = VirtualClock()
    c.register()
    t = c.start_thread(lambda: c.wait_until(None, lambda: False), "waiter")
    c.close()
    t.join(2.0)
    assert not t.is_alive()
    with pytest.raises(ClockClosed):
        c.wait_until(c.monotonic() + 1.0)
    assert threading.active_count() >= 1

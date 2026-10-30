"""장비 worker (장비 공통) — 포트 하나당 스레드 하나가 포트를 독점한다.

- 요청은 우선순위 큐로: safety(0) > write(1) > read(2) > low(3), 같은 우선순위는 넣은 순서. 모든 요청에 deadline.
  low = 양보하는 백그라운드 읽기(챔버12 PLC 1초 묶음을 블록마다 나눈 요청 — v1 PLC priority='low' 가 블록 사이에서 기다리는
  요청에 양보하던 것, device/plc.py:1485-1494·2257-2310). 기다리는 다른 요청이 모두 먼저 돈다.
- safety 요청(purge=True)은 큐의 읽기·쓰기를 버리고(CANCELLED) 진행 중인 읽기를 취소한 뒤 맨 앞에서 실행.
- 연결 상태 기계: disconnected → (보호된 열기) → connected. 실패하면 지수 백오프(시작·최대·장기 두절은 파라미터).
  연속 무응답 fail_streak 회 또는 포트 오류면 끊고 재연결. 장기 두절은 알림 1회, 기록은 log_every_s 마다.
- 긴 쓰기 시퀀스는 Sequence 로: 단계 하나 실행 → 다음 단계를 gap_s 뒤(not_before)에 다시 큐에 넣는다 —
  그 사이 tick 의 읽기가 끼어든다. 단계가 실패하거나 safety 가 오면 시퀀스 중단.
- bench 모드(writes_allowed=False): write·safety 요청은 BENCH_READ_ONLY 로 즉시 거부.
- 억제 구간(suppress): 예정된 무응답(예: 이온 게이지 필라멘트 전환) — 무응답을 실패로 세지 않는다.
- 이벤트(on_event)와 요청 결과(on_done)는 worker 스레드에서 부른다 — 받는 쪽은 짧게 처리할 것.
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Callable, List, Optional

from v2.common.clock import ClockQueue
from v2.common.transport import Cancelled, CancelToken, CommandBlocked, ReadTimeout, Transport, TransportError


class Priority(IntEnum):
    SAFETY = 0
    WRITE = 1
    READ = 2
    LOW = 3


_KIND_PRIO = {"safety": Priority.SAFETY, "write": Priority.WRITE, "read": Priority.READ, "low": Priority.LOW}
_READ_KINDS = ("read", "low")


class Status:
    OK = "ok"
    TIMEOUT = "timeout"
    FAIL = "fail"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    LINK_DOWN = "link_down"


@dataclass
class Request:
    kind: str                                   # read / write / safety / low(양보하는 백그라운드 읽기)
    fn: Callable[["Session"], Any]
    deadline: Optional[float] = None            # 이 시각까지 끝나야 함(시작 전 지나면 EXPIRED)
    on_done: Optional[Callable[["Result"], None]] = None
    name: str = ""
    tick_seq: Optional[int] = None
    not_before: Optional[float] = None
    purge: bool = False                         # safety: 큐 비우기·진행 중 읽기 취소
    exec_timeout_s: Optional[float] = None      # 실행 시작 기준 마감(시퀀스 단계) — 없으면 deadline
    confirms_link: bool = True                  # 성공하면 통신 정상으로 셈. 응답 없는 쓰기는 False(장비가 답해야 정상)
    id: int = field(default_factory=lambda: next(_ids))


_ids = itertools.count(1)


@dataclass
class Result:
    request: Request
    status: str
    value: Any = None
    reason: Optional[str] = None                # 사유 코드(v2.common.commands)
    error: str = ""
    started: Optional[float] = None
    finished: Optional[float] = None


@dataclass(frozen=True)
class WorkerEvent:
    device: str
    kind: str        # connected / connect_failed / disconnected / long_outage / timeout / recovered
    detail: str
    t: float


CONNECT_RETRY_PAUSE_S = 0.3        # 연결 초기화 무응답 뒤 다시 하기 전 쉼

class Session:
    """드라이버가 요청 하나 동안 쓰는 입출력. deadline·취소가 자동으로 걸린다."""

    def __init__(self, transport: Transport, deadline: float, cancel: CancelToken) -> None:
        self.transport = transport
        self.clock = transport.clock
        self.deadline = deadline
        self.cancel = cancel
        self.rx_bytes = 0                  # 이 세션에서 받은 바이트 수(연결 초기화 재시도 판단)

    def write(self, data: bytes) -> None:
        if self.cancel.is_set():
            raise Cancelled()
        self.transport.write(data)

    def read_until(self, terminator: Optional[bytes] = None, size: Optional[int] = None,
                   deadline: Optional[float] = None) -> bytes:
        d = self.deadline if deadline is None else min(deadline, self.deadline)
        try:
            out = self.transport.read_until(d, terminator, size, self.cancel)
        except ReadTimeout as e:
            self.rx_bytes += len(e.partial)
            raise
        self.rx_bytes += len(out)
        return out


class Backoff:
    """재연결 간격 — v1 lib/comm_policy.ReconnectPolicy 를 다시 쓴 것."""

    def __init__(self, clock, start_s: float, max_s: float, long_outage_s: float, probe_s: float,
                 log_every_s: float) -> None:
        self.clock = clock
        self.start_s, self.max_s = start_s, max_s
        self.long_outage_s, self.probe_s, self.log_every_s = long_outage_s, probe_s, log_every_s
        self._n = 0
        self._first: Optional[float] = None
        self._alerted = False
        self._last_log: Optional[float] = None

    def on_failure(self) -> float:
        now = self.clock.monotonic()
        if self._first is None:
            self._first = now
        if self.is_long_outage():
            return self.probe_s
        d = min(self.start_s * (2 ** self._n), self.max_s)
        self._n += 1
        return d

    def on_success(self) -> None:
        self._n, self._first, self._alerted, self._last_log = 0, None, False, None

    def outage_s(self) -> float:
        return 0.0 if self._first is None else self.clock.monotonic() - self._first

    def is_long_outage(self) -> bool:
        return self._first is not None and self.outage_s() >= self.long_outage_s

    def take_alert(self) -> bool:
        if self.is_long_outage() and not self._alerted:
            self._alerted = True
            return True
        return False

    def should_log(self) -> bool:
        if not self.is_long_outage():
            return True
        now = self.clock.monotonic()
        if self._last_log is None or now - self._last_log >= self.log_every_s:
            self._last_log = now
            return True
        return False


SEQ_DONE = object()      # 시퀀스 단계 fn 이 이것을 돌려주면 남은 단계 없이 성공으로 끝낸다


class Sequence:
    """여러 tick 에 걸쳐 한 단계씩 실행하는 쓰기. steps = [(이름, fn(session))].
    단계 fn 이 SEQ_DONE 을 돌려주면 성공으로 일찍 끝내고, 예외면 실패로 끝낸다."""

    def __init__(self, name: str, steps: List[tuple], gap_s: float = 0.0, step_timeout_s: float = 0.5,
                 on_done: Optional[Callable[[str, Optional[str], List[Result]], None]] = None,
                 confirms_link: bool = True, start_after: Optional[float] = None) -> None:
        self.name, self.steps, self.gap_s, self.step_timeout_s = name, list(steps), gap_s, step_timeout_s
        self.on_done = on_done
        self.confirms_link = confirms_link
        self.start_after = start_after          # 첫 단계 not_before(예: 같은 tick 의 읽기 뒤)
        self.results: List[Result] = []
        self.finished = False


class Worker:
    def __init__(self, name: str, transport: Transport, *, writes_allowed: bool = True,
                 fail_streak: int = 3, reconnect_start_s: float = 1.0, reconnect_max_s: float = 60.0,
                 long_outage_s: float = 600.0, probe_s: float = 60.0, log_every_s: float = 600.0,
                 on_event: Optional[Callable[[WorkerEvent], None]] = None,
                 on_connect: Optional[Callable[[Session], None]] = None,
                 connect_init_timeout_s: float = 5.0) -> None:
        self.name = name
        self.transport = transport
        self.clock = transport.clock
        self.writes_allowed = writes_allowed
        self.fail_streak = fail_streak
        self.backoff = Backoff(self.clock, reconnect_start_s, reconnect_max_s, long_outage_s, probe_s, log_every_s)
        self.on_event = on_event
        self.on_connect = on_connect
        self.connect_init_timeout_s = connect_init_timeout_s
        self.queue = ClockQueue(self.clock)
        self.connected = False
        self.last_ok: Optional[float] = None
        self.inflight: Optional[Request] = None
        self._cancel: Optional[CancelToken] = None
        self._streak = 0
        self._next_attempt: float = self.clock.monotonic()
        self._suppress_until: Optional[float] = None
        self.suppress_reason = ""
        self._stop = False
        self._thread = None
        self._seqs: List[Sequence] = []
        self.initializing = False      # 연결 직후 초기화(on_connect) 중

    # ───────── 생명주기 ─────────
    def start(self) -> None:
        self._thread = self.clock.start_thread(self._run, f"worker-{self.name}")

    def stop(self, join_s: float = 5.0) -> None:
        with self.clock.cond:
            self._stop = True
            if self._cancel is not None:      # 진행 중 요청을 끊어야 기다림 없이 끝난다(가상 시계에서도)
                self._cancel.set()
            self.clock.cond.notify_all()
        if self._thread is not None:
            self._thread.join(join_s)
        # 큐에 남은 요청도 결과를 돌려준다(CANCELLED) — 기다리는 쪽(스냅샷 묶기의 '진행 중' 등)이 영영 기다리지 않게(챔버12 2단계에서 찾음)
        with self.clock.cond:
            left = self.queue.drain(keep=lambda r: False)
        now = self.clock.monotonic()
        for r in left:
            self._finish(Result(r, Status.CANCELLED, reason="CANCELLED", started=now, finished=now))

    # ───────── 상태 ─────────
    def suppress(self, until: float, reason: str) -> None:
        with self.clock.cond:
            self._suppress_until, self.suppress_reason = until, reason

    def suppressed(self, now: Optional[float] = None) -> bool:
        now = self.clock.monotonic() if now is None else now
        return self._suppress_until is not None and now < self._suppress_until

    @property
    def busy(self) -> bool:
        return self.inflight is not None

    @property
    def running(self) -> bool:
        """스레드가 돌고 있고 멈추는 중이 아니다(start 뒤 stop 전) — 연결 여부와 별개."""
        t = self._thread
        return t is not None and t.is_alive() and not self._stop

    @property
    def stopped(self) -> bool:
        """stop 을 불렀고 스레드가 끝났다(또는 시작한 적이 없다) — 포트를 놓았다."""
        t = self._thread
        return t is None or not t.is_alive()

    @property
    def ready(self) -> bool:
        """연결됐고 연결 직후 초기화가 끝났다."""
        return self.connected and not self.initializing

    # ───────── 요청 ─────────
    def submit(self, req: Request) -> None:
        if req.kind not in _KIND_PRIO:
            raise ValueError(req.kind)
        if req.kind in ("write", "safety") and not self.writes_allowed:
            self._finish(Result(req, Status.REJECTED, reason="BENCH_READ_ONLY"))
            return
        dropped: List[Request] = []
        with self.clock.cond:
            if req.kind == "safety" and req.purge:
                dropped = self.queue.drain(keep=lambda r: r.kind == "safety")
                if self.inflight is not None and self.inflight.kind in _READ_KINDS and self._cancel is not None:
                    self._cancel.set()
                for s in self._seqs:
                    s.finished = True
                self._seqs = []
            self.queue.put(int(_KIND_PRIO[req.kind]), req, req.not_before)
        now = self.clock.monotonic()
        for r in dropped:
            self._finish(Result(r, Status.CANCELLED, reason="CANCELLED", started=now, finished=now))

    def submit_sequence(self, seq: Sequence) -> None:
        if not self.writes_allowed:
            if seq.on_done:
                seq.on_done(seq.name, "BENCH_READ_ONLY", [])
            return
        with self.clock.cond:
            self._seqs.append(seq)
        self._queue_step(seq, 0, seq.start_after)

    def _queue_step(self, seq: Sequence, i: int, not_before: Optional[float]) -> None:
        name, fn = seq.steps[i]

        def done(res: Result) -> None:
            seq.results.append(res)
            if seq.finished:
                return
            if res.status != Status.OK:
                self._end_seq(seq, res.reason or res.status)
            elif res.value is SEQ_DONE:                      # 단계가 일찍 끝냄(성공)
                self._end_seq(seq, None)
            elif i + 1 < len(seq.steps):
                self._queue_step(seq, i + 1, self.clock.monotonic() + seq.gap_s)
            else:
                self._end_seq(seq, None)

        req = Request("write", fn, on_done=done, name=f"{seq.name}:{name}", not_before=not_before,
                      exec_timeout_s=seq.step_timeout_s, confirms_link=seq.confirms_link)
        self.submit(req)

    def _end_seq(self, seq: Sequence, reason: Optional[str]) -> None:
        with self.clock.cond:
            seq.finished = True
            if seq in self._seqs:
                self._seqs.remove(seq)
        if seq.on_done:
            seq.on_done(seq.name, reason, seq.results)

    # ───────── 스레드 ─────────
    def _emit(self, kind: str, detail: str = "") -> None:
        if self.on_event:
            self.on_event(WorkerEvent(self.name, kind, detail, self.clock.monotonic()))

    def _finish(self, res: Result) -> None:
        if res.request.on_done:
            res.request.on_done(res)

    def _run(self) -> None:
        while not self._stop:
            if not self.connected and self.clock.monotonic() >= self._next_attempt:
                self._try_connect()
            until = None if self.connected else self._next_attempt
            req = self.queue.get(until, stop=lambda: self._stop)
            if req is not None:
                self._execute(req)
        if self.connected:
            self.transport.close()

    def _try_connect(self) -> None:
        try:
            self.transport.open()
        except TransportError as e:
            delay = self.backoff.on_failure()
            self._next_attempt = self.clock.monotonic() + delay
            if self.backoff.take_alert():
                self._emit("long_outage", f"{self.backoff.outage_s():.0f}s")
            if self.backoff.should_log():
                self._emit("connect_failed", f"{e} — {delay:g}s 뒤 재시도")
            return
        was_outage = self.backoff.outage_s()
        self.connected = True
        self._streak = 0
        self.backoff.on_success()
        self._emit("connected", f"단절 {was_outage:.1f}s 뒤" if was_outage else "")
        if self.on_connect:
            cancel = CancelToken(self.clock)
            with self.clock.cond:
                self._cancel = cancel          # stop()·safety 가 초기화도 끊을 수 있게
                self.initializing = True
            sess = Session(self.transport, self.clock.monotonic() + self.connect_init_timeout_s, cancel)
            try:
                try:
                    self.on_connect(sess)
                except ReadTimeout as e:
                    if sess.rx_bytes or cancel.is_set():
                        raise
                    # 초기화 중 받은 바이트가 하나도 없이 시간 초과 — 연결 직후 첫 질의를 장비가 놓친 경우(10/16 bench DC).
                    # 포트를 닫지 않고 0.3 s 뒤 입력을 비우고 초기화를 1회 다시. 그래도 안 되면 끊고 백오프
                    self._emit("connect_retry", "초기화 첫 질의 무응답 — 0.3 s 뒤 1회 다시"
                               + (f" (받다 만 바이트 {e.partial.hex(' ')})" if e.partial else ""))
                    self.clock.wait_until(self.clock.monotonic() + CONNECT_RETRY_PAUSE_S, lambda: cancel.is_set())
                    self.transport.reset_input()
                    sess = Session(self.transport, self.clock.monotonic() + self.connect_init_timeout_s, cancel)
                    self.on_connect(sess)
            except Cancelled:
                self.transport.reset_input()
            except (TransportError, ReadTimeout) as e:
                self._disconnect(f"연결 초기화 실패: {e}")
            except Exception as e:             # 드라이버 초기화 오류(형식·되읽기 불일치 등) — 끊고 다시
                self._disconnect(f"연결 초기화 오류: {e!r}")
            finally:
                with self.clock.cond:
                    self._cancel = None
                    self.initializing = False
                    self.clock.cond.notify_all()

    def _disconnect(self, why: str) -> None:
        self.transport.close()
        self.connected = False
        delay = self.backoff.on_failure()
        self._next_attempt = self.clock.monotonic() + delay
        self._emit("disconnected", f"{why} — {delay:g}s 뒤 재연결")

    def _execute(self, req: Request) -> None:
        now = self.clock.monotonic()
        if req.deadline is not None and now > req.deadline:
            self._finish(Result(req, Status.EXPIRED, reason="EXPIRED", started=now, finished=now))
            return
        if not self.connected and now >= self._next_attempt:
            self._try_connect()            # 재연결 시각이 된 뒤 온 요청은 먼저 연결을 시도
        if not self.connected:
            self._finish(Result(req, Status.LINK_DOWN, reason="LINK_DOWN", started=now, finished=now))
            return
        cancel = CancelToken(self.clock)
        if req.exec_timeout_s is not None:
            deadline = now + req.exec_timeout_s
        else:
            deadline = req.deadline if req.deadline is not None else now + 1.0
        with self.clock.cond:
            self.inflight, self._cancel = req, cancel
        res = Result(req, Status.FAIL, started=now)
        try:
            res.value = req.fn(Session(self.transport, deadline, cancel))
            res.status = Status.OK
            if req.confirms_link:
                self._streak = 0
                self.last_ok = self.clock.monotonic()
        except Cancelled:
            res.status, res.reason = Status.CANCELLED, "CANCELLED"
            self.transport.reset_input()
        except ReadTimeout as e:
            res.status, res.reason, res.error = Status.TIMEOUT, "DEVICE_NO_RESPONSE", str(e)
            self.transport.reset_input()
            if not self.suppressed():
                self._streak += 1
                self._emit("timeout", f"{req.name} 무응답 {self._streak}회")
                if self._streak >= self.fail_streak:
                    self._disconnect(f"무응답 {self._streak}회 연속")
        except TransportError as e:
            res.status, res.reason, res.error = Status.FAIL, "LINK_DOWN", str(e)
            self._disconnect(str(e))
        except Exception as e:      # 드라이버 오류(응답 형식 등)·가드가 막은 명령 — 연결은 유지
            res.status, res.error = Status.FAIL, repr(e)
            res.reason = "COMMAND_BLOCKED" if isinstance(e, CommandBlocked) else "DEVICE_REJECTED"
            self.transport.reset_input()
        finally:
            with self.clock.cond:
                self.inflight, self._cancel = None, None
        res.finished = self.clock.monotonic()
        self._finish(res)

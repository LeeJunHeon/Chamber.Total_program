"""전송 계층 (장비 공통).

드라이버가 쓰는 입출력은 write(bytes) 와 read_until(deadline, terminator|size) 두 가지뿐이다.
open·close·reset_input 은 worker 의 연결 관리만 쓴다.

- SerialTransport : pyserial. 포트 열기·닫기는 PortGuard 로 — 제한 시간이 있는 별도 스레드에서
                    (2026-09-16 사고: USB-시리얼이 사라진 뒤 close/open 이 30초씩 막혀 워커가 멈춤).
- SimTransport    : 가짜 장비 모델(v2.common.simdev). 응답 지연·무응답·끊김을 가상 시각으로 재현.
- ReplayTransport : 녹화(RecordingTransport)를 재생. 보낸 바이트가 녹화와 다르면 ReplayMismatch.

시각은 모두 clock.monotonic() 기준. 기다림은 clock.wait_until 로만 — 가상 시계에서도 같은 코드가 돈다.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple


class TransportError(OSError):
    """포트·장치 수준 오류 — worker 가 연결을 끊고 재연결한다."""


class CommandBlocked(Exception):
    """통신층 가드가 보내기 전에 막은 명령(포트에 닿지 않음). 연결 이상이 아니다."""
    reason = "COMMAND_BLOCKED"


class ReadTimeout(Exception):
    def __init__(self, partial: bytes = b"") -> None:
        super().__init__(f"read timeout (partial {len(partial)} bytes)")
        self.partial = partial


class Cancelled(Exception):
    """안전 요청이 진행 중인 읽기를 취소함."""


class CancelToken:
    def __init__(self, clock) -> None:
        self._clock = clock
        self._set = False

    def set(self) -> None:
        with self._clock.cond:
            self._set = True
            self._clock.cond.notify_all()

    def is_set(self) -> bool:
        return self._set


def _split(buf: bytes, terminator: Optional[bytes], size: Optional[int]) -> Optional[Tuple[bytes, bytes]]:
    """(한 응답, 나머지) 또는 아직 모자라면 None."""
    if terminator:
        i = buf.find(terminator)
        if i >= 0:
            j = i + len(terminator)
            return buf[:j], buf[j:]
        return None
    if size is not None and len(buf) >= size:
        return buf[:size], buf[size:]
    return None


class Transport:
    """추상 전송."""

    def __init__(self, clock) -> None:
        self.clock = clock
        self._rx = b""

    # 연결 관리(worker 전용)
    def open(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    @property
    def is_open(self) -> bool:
        raise NotImplementedError

    def reset_input(self) -> None:
        self._rx = b""

    # 드라이버 입출력
    def write(self, data: bytes) -> None:
        raise NotImplementedError

    def read_until(self, deadline: float, terminator: Optional[bytes] = None, size: Optional[int] = None,
                   cancel: Optional[CancelToken] = None) -> bytes:
        raise NotImplementedError


# ───────────── 포트 열기·닫기 보호 ─────────────
class PortGuard:
    """factory() 를 데몬 스레드에서 실행하고 timeout_s(실제 초) 안에 끝나지 않으면 포기한다.
    늦게 열린 객체는 그 스레드가 스스로 닫는다. 이전 열기가 아직 막혀 있으면 새 열기를 시작하지 않는다
    (막힌 스레드가 쌓이지 않게)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stuck = 0

    @property
    def stuck(self) -> int:
        return self._stuck

    def open(self, factory: Callable[[], Any], timeout_s: float) -> Any:
        with self._lock:
            if self._stuck:
                raise TransportError("이전 포트 열기가 아직 끝나지 않음")
        box = {"obj": None, "err": None, "done": False, "late": False}
        lock = threading.Lock()
        done = threading.Event()

        def run():
            obj, err = None, None
            try:
                obj = factory()
            except Exception as e:
                err = e
            with lock:
                box["done"] = True
                late = box["late"]
                if not late:
                    box["obj"], box["err"] = obj, err
            if late:
                if obj is not None:
                    _close_quietly(obj)
                with self._lock:
                    self._stuck -= 1
            done.set()

        threading.Thread(target=run, name="port-open", daemon=True).start()
        done.wait(timeout_s)
        with lock:
            if not box["done"]:
                box["late"] = True
                with self._lock:
                    self._stuck += 1
                raise TransportError(f"포트 열기 {timeout_s:g}s 초과")
        if box["err"] is not None:
            raise TransportError(f"포트 열기 실패: {box['err']!r}")
        return box["obj"]

    @staticmethod
    def close(obj: Any) -> None:
        """닫기는 데몬 스레드에 넘기고 바로 돌아온다."""
        if obj is not None:
            threading.Thread(target=_close_quietly, args=(obj,), name="port-close", daemon=True).start()


def _close_quietly(obj: Any) -> None:
    try:
        obj.close()
    except Exception:
        pass


# ───────────── pyserial ─────────────
@dataclass(frozen=True)
class SerialSettings:
    port: str
    baudrate: int
    bytesize: int = 8
    parity: str = "N"          # N / O / E
    stopbits: float = 1
    write_timeout: float = 1.0


class SerialTransport(Transport):
    POLL_S = 0.005

    def __init__(self, clock, settings: SerialSettings, open_timeout_s: float = 2.0,
                 serial_factory: Optional[Callable[[SerialSettings], Any]] = None,
                 guard: Optional[PortGuard] = None) -> None:
        super().__init__(clock)
        self.settings = settings
        self.open_timeout_s = open_timeout_s
        self._factory = serial_factory or _pyserial_factory
        self._guard = guard or PortGuard()
        self._ser = None

    @property
    def is_open(self) -> bool:
        return self._ser is not None

    def open(self) -> None:
        if self._ser is not None:
            return
        self._ser = self._guard.open(lambda: self._factory(self.settings), self.open_timeout_s)
        self._rx = b""

    def close(self) -> None:
        ser, self._ser = self._ser, None
        self._rx = b""
        PortGuard.close(ser)

    def reset_input(self) -> None:
        self._rx = b""
        if self._ser is not None:
            try:
                self._ser.reset_input_buffer()
            except Exception as e:
                raise TransportError(f"reset_input: {e!r}") from e

    def write(self, data: bytes) -> None:
        if self._ser is None:
            raise TransportError("포트가 닫혀 있음")
        try:
            self._ser.write(data)
        except Exception as e:
            raise TransportError(f"write: {e!r}") from e

    def read_until(self, deadline: float, terminator: Optional[bytes] = None, size: Optional[int] = None,
                   cancel: Optional[CancelToken] = None) -> bytes:
        if self._ser is None:
            raise TransportError("포트가 닫혀 있음")
        while True:
            got = _split(self._rx, terminator, size)
            if got is not None:
                out, self._rx = got
                return out
            try:
                n = self._ser.in_waiting
                if n:
                    self._rx += self._ser.read(n)
                    continue
            except Exception as e:
                raise TransportError(f"read: {e!r}") from e
            now = self.clock.monotonic()
            if cancel is not None and cancel.is_set():
                raise Cancelled()
            if now >= deadline:
                partial, self._rx = self._rx, b""
                raise ReadTimeout(partial)
            # time.sleep(고해상도)로 짧게 — 조건 변수 대기는 Windows 에서 약 15 ms 단위라 쓰지 않는다
            self.clock.sleep_until(min(deadline, now + self.POLL_S))


def _pyserial_factory(s: SerialSettings) -> Any:
    import serial  # pyserial — 실제 포트는 bench 단계부터 연다
    return serial.Serial(port=s.port, baudrate=s.baudrate, bytesize=s.bytesize, parity=s.parity,
                         stopbits=s.stopbits, timeout=0, write_timeout=s.write_timeout)


# ───────────── 예정된 수신이 있는 전송(가짜 장비·재생 공통) ─────────────
class _ScheduledTransport(Transport):
    def __init__(self, clock) -> None:
        super().__init__(clock)
        self._sched: List[Tuple[float, bytes]] = []   # (도착 시각, 바이트)
        self._open = False

    @property
    def is_open(self) -> bool:
        return self._open

    def reset_input(self) -> None:
        self._rx = b""
        self._sched = []

    def _link_ok(self) -> bool:
        return True

    def _pull(self) -> None:
        now = self.clock.monotonic()
        keep = []
        for t, b in self._sched:
            if t <= now:
                self._rx += b
            else:
                keep.append((t, b))
        self._sched = keep

    def read_until(self, deadline: float, terminator: Optional[bytes] = None, size: Optional[int] = None,
                   cancel: Optional[CancelToken] = None) -> bytes:
        if not self._open:
            raise TransportError("포트가 닫혀 있음")
        while True:
            self._pull()
            got = _split(self._rx, terminator, size)
            if got is not None:
                out, self._rx = got
                return out
            if not self._link_ok():
                raise TransportError("장치 끊김")
            if cancel is not None and cancel.is_set():
                raise Cancelled()
            now = self.clock.monotonic()
            if now >= deadline:
                partial, self._rx = self._rx, b""
                raise ReadTimeout(partial)
            nxt = min([t for t, _ in self._sched], default=None)
            target = deadline if nxt is None else min(deadline, nxt)
            nl = self._next_link_change()
            if nl is not None:
                target = min(target, nl)
            self.clock.wait_until(target, (lambda: cancel.is_set()) if cancel is not None else None)

    def _next_link_change(self) -> Optional[float]:
        return None


class SimTransport(_ScheduledTransport):
    def __init__(self, clock, device) -> None:
        super().__init__(clock)
        self.device = device

    def _link_ok(self) -> bool:
        return self.device.is_connected(self.clock.monotonic())

    def _next_link_change(self) -> Optional[float]:
        return self.device.next_link_change(self.clock.monotonic())

    def open(self) -> None:
        if not self._link_ok():
            raise TransportError(f"{self.device.name}: 장치 없음")
        self._open = True
        self._rx, self._sched = b"", []

    def close(self) -> None:
        self._open = False
        self._rx, self._sched = b"", []

    def write(self, data: bytes) -> None:
        if not self._open:
            raise TransportError("포트가 닫혀 있음")
        now = self.clock.monotonic()
        if not self._link_ok():
            raise TransportError(f"{self.device.name}: 장치 끊김")
        self._sched += self.device.respond(data, now)
        self._sched.sort(key=lambda x: x[0])


# ───────────── 녹화·재생 ─────────────
@dataclass(frozen=True)
class Exchange:
    tx: bytes
    rx: bytes            # 빈 바이트면 무응답
    delay_s: float       # 보낸 뒤 rx 가 다 도착하기까지
    t_s: float = 0.0     # 녹화 시작 뒤 보낸 시각


def save_exchanges(path, exchanges: List[Exchange]) -> None:
    """녹화를 JSON lines 로(보낸·받은 바이트 hex, 보낸 시각, 응답 지연). bench 에서 녹화해 재생 시험에 쓴다."""
    import json
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for e in exchanges:
            f.write(json.dumps({"t_s": round(e.t_s, 6), "tx": e.tx.hex(), "rx": e.rx.hex(),
                                "delay_s": round(e.delay_s, 6)}) + "\n")


def load_exchanges(path) -> List[Exchange]:
    import json
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                o = json.loads(line)
                out.append(Exchange(bytes.fromhex(o["tx"]), bytes.fromhex(o["rx"]), o["delay_s"], o["t_s"]))
    return out


class ReplayMismatch(AssertionError):
    pass


class RecordingTransport(Transport):
    """다른 전송을 감싸 주고받은 내용을 Exchange 목록으로 남긴다(bench 녹화용)."""

    def __init__(self, inner: Transport) -> None:
        super().__init__(inner.clock)
        self.inner = inner
        self.exchanges: List[Exchange] = []
        self._t0: Optional[float] = None
        self._tx: Optional[Tuple[float, bytes]] = None
        self._rx_acc = b""

    @property
    def is_open(self) -> bool:
        return self.inner.is_open

    def open(self) -> None:
        self.inner.open()

    def close(self) -> None:
        self._flush()
        self.inner.close()

    def save(self, path) -> None:
        self._flush()
        save_exchanges(path, self.exchanges)

    def reset_input(self) -> None:
        self.inner.reset_input()

    def _flush(self) -> None:
        if self._tx is not None:
            t0, tx = self._tx
            self.exchanges.append(Exchange(tx, self._rx_acc, self._last_rx_t - t0 if self._rx_acc else 0.0,
                                           t0 - (self._t0 or t0)))
            self._tx, self._rx_acc = None, b""

    def write(self, data: bytes) -> None:
        self._flush()
        self.inner.write(data)
        if self._t0 is None:
            self._t0 = self.clock.monotonic()
        self._tx = (self.clock.monotonic(), data)
        self._last_rx_t = self.clock.monotonic()

    def read_until(self, deadline, terminator=None, size=None, cancel=None) -> bytes:
        out = self.inner.read_until(deadline, terminator, size, cancel)
        self._rx_acc += out
        self._last_rx_t = self.clock.monotonic()
        return out


class ReplayTransport(_ScheduledTransport):
    def __init__(self, clock, exchanges: List[Exchange]) -> None:
        super().__init__(clock)
        self._ex = list(exchanges)
        self._i = 0

    def open(self) -> None:
        self._open = True

    def close(self) -> None:
        self._open = False

    @property
    def remaining(self) -> int:
        return len(self._ex) - self._i

    def write(self, data: bytes) -> None:
        if self._i >= len(self._ex):
            raise ReplayMismatch(f"녹화보다 많이 보냄: {data!r}")
        ex = self._ex[self._i]
        if ex.tx != data:
            raise ReplayMismatch(f"{self._i}번째: 녹화 {ex.tx!r} ≠ 보냄 {data!r}")
        self._i += 1
        if ex.rx:
            self._sched.append((self.clock.monotonic() + ex.delay_s, ex.rx))
            self._sched.sort(key=lambda x: x[0])

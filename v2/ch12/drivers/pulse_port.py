"""펄스 전원 포트 소유(챔버12) — DC Pulse·RF Pulse 가 같은 MOXA NPort 포트를 나눠 쓴다.

v1 사실
- CH1 은 DC Pulse·RF Pulse 둘 다 192.168.1.50:4007, CH2 는 4005(DC Pulse 는 MOXA 쪽 Parity None + Null Modem 으로
  케이블을 바꿔 씀) — devices_v1facts.FACTS["dcpulse"/"rfpulse"].connection, config/settings.json:566-567, lib/config_ch1.py:17.
- 공정이 그 펄스 장비를 고를 때만(use_dc_pulse/use_rf_pulse) 연결하고, 같은 주소를 '다른 챔버' 또는 '같은 챔버의 다른 종류'가
  쓰고 있으면 시작을 거부한다. 같은 챔버·같은 종류의 다시 잡기는 허용. 해제는 공정 끝(챔버 단위로 한꺼번에)
  (v1 controller/runtime_state.py:110-133, runtime/chamber_runtime.py:3864-3894·3247).
  거부 문구: "{DC-Pulse|RF-Pulse} 시작 불가: 동일 주소({host:port}) 장비를 CH{n}({종류}) 공정이 사용 중입니다."

v2 구조
- PulsePorts: 주소('host:port' 소문자) → PulsePort. 드라이버를 붙일 때 그 드라이버의 주소로 포트를 찾는다(같은 주소면 같은 포트).
- PulsePort.claim(kind, chamber): 위 규칙으로 점유. 처음 잡으면 그 종류의 전송(transport_factory)과 Worker 를 새로 만들어
  드라이버에 붙이고(driver.bind) 스냅샷 소스의 worker 를 채운다 → 그때부터 1초 읽기·명령. 해제(release)하면 Worker 를 멈추고
  소스 worker 를 None 으로 → 1초 행에서 off 품질(쓰지 않는 펄스 장비는 연결하지 않는다 — v1 과 같음).
- OpRunner: 여러 I/O 로 된 동작(시작 순서·OFF 확인 등)을 generator 로 쓰고, I/O 하나를 worker 요청 하나로 넣는다
  (그 사이 tick 읽기가 끼어들 수 있다). 기다림(Sleep)도 빈 요청(not_before)으로 넣어 안전 요청(purge)이 오면 같이 취소된다.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

from v2.common.worker import Request, Result, Status, Worker

KIND_NAME = {"dcpulse": "DC-Pulse", "rfpulse": "RF-Pulse"}       # v1 runtime 문구의 종류 이름


# ───────────── 동작(op) 실행기 ─────────────
@dataclass
class IO:
    """worker 요청 하나. fn(session) 의 반환값이 결과 value."""
    fn: Callable[[Any], Any]
    limit_s: float
    kind: str = "write"            # write / safety
    purge: bool = False
    confirms_link: bool = True
    name: str = ""


@dataclass
class Sleep:
    s: float


@dataclass
class WaitLink:
    """worker 가 연결될 때까지(최대 timeout_s) — 결과 True/False. v1 _wait_until_connected 대응."""
    timeout_s: float


@dataclass
class StepResult:
    status: str
    value: Any = None
    reason: Optional[str] = None
    error: str = ""

    @property
    def ok(self) -> bool:
        return self.status == Status.OK


class OpFail(Exception):
    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(detail or reason)
        self.reason, self.detail = reason, detail


_NOOP = (lambda s: None)
WAIT_LINK_POLL_S = 0.1


class OpRunner:
    """generator 하나를 끝까지 돌린다. on_done(reason|None, value) 는 worker 스레드(또는 호출 스레드)에서 한 번."""

    def __init__(self, clock, worker: Worker, name: str, gen, on_done: Callable[[Optional[str], Any], None],
                 base_kind: str = "write") -> None:
        self.clock, self.worker, self.name, self.gen = clock, worker, name, gen
        self.on_done = on_done
        self.base_kind = base_kind
        self.done = False
        self._lock = threading.RLock()

    def start(self) -> "OpRunner":
        self._advance(None)
        return self

    def abort(self, reason: str) -> None:
        """밖에서 끝냄(포트 해제 등). generator 는 닫는다."""
        with self._lock:
            if self.done:
                return
            self.done = True
        try:
            self.gen.close()
        except Exception:
            pass
        self.on_done(reason, None)

    def _finish(self, reason: Optional[str], value: Any = None) -> None:
        with self._lock:
            if self.done:
                return
            self.done = True
        self.on_done(reason, value)

    def _advance(self, send_value) -> None:
        with self._lock:
            if self.done:
                return
            try:
                item = self.gen.send(send_value)
            except StopIteration as e:
                self._finish(None, e.value)
                return
            except OpFail as e:
                self._finish(e.reason, e.detail)
                return
            except Exception as e:                       # 드라이버 내부 오류도 op 실패로(스레드가 죽지 않게)
                self._finish("INTERNAL_ERROR", repr(e))
                return
        self._submit(item)

    def _submit(self, item) -> None:
        now = self.clock.monotonic()
        if isinstance(item, IO):
            req = Request(item.kind, item.fn, name=f"{self.name}:{item.name}", exec_timeout_s=item.limit_s,
                          purge=item.purge, confirms_link=item.confirms_link, on_done=self._on_io)
        elif isinstance(item, Sleep):
            req = Request(self.base_kind, _NOOP, name=f"{self.name}:sleep", not_before=now + max(0.0, item.s),
                          confirms_link=False, on_done=self._on_sleep)
        elif isinstance(item, WaitLink):
            self._wait_link(now + max(0.0, item.timeout_s))
            return
        else:
            self._finish("INTERNAL_ERROR", f"알 수 없는 단계 {item!r}")
            return
        self.worker.submit(req)

    def _on_io(self, r: Result) -> None:
        if r.status in (Status.CANCELLED, Status.REJECTED):
            self._close_with(r.reason or r.status)
            return
        self._advance(StepResult(r.status, r.value, r.reason, r.error))

    def _on_sleep(self, r: Result) -> None:
        if r.status in (Status.CANCELLED, Status.REJECTED):
            self._close_with(r.reason or r.status)
            return
        self._advance(None)

    def _wait_link(self, until: float) -> None:
        w = self.worker
        if w.connected and not w.initializing:
            self._advance(True)
            return
        if self.clock.monotonic() >= until:
            self._advance(False)
            return

        def again(r: Result) -> None:
            if r.status in (Status.CANCELLED, Status.REJECTED):
                self._close_with(r.reason or r.status)
                return
            self._wait_link(until)
        nb = min(until, self.clock.monotonic() + WAIT_LINK_POLL_S)
        w.submit(Request(self.base_kind, _NOOP, name=f"{self.name}:wait_link", not_before=nb,
                         confirms_link=False, on_done=again))

    def _close_with(self, reason: str) -> None:
        with self._lock:
            if self.done:
                return
            self.done = True
        try:
            self.gen.close()
        except Exception:
            pass
        self.on_done(reason, None)


def wait_then(clock, until: float, done: Callable[[], bool], then: Callable[[bool], None], name: str) -> None:
    """done() 이 참이 되거나 until 이 지나면 then(참이 됐는지) — worker 와 따로 도는 짧은 스레드(정리 한도처럼 worker 가
    긴 I/O 중이어도 제시간에 끊어야 할 때. v1 asyncio.wait_for 대응). 상태를 바꾼 쪽은 clock.notify() 를 부른다."""
    def run():
        then(bool(clock.wait_until(until, done)))
    clock.start_thread(run, name)


# ───────────── 포트 소유 ─────────────
def norm_endpoint(host: str, port: int) -> str:
    return f"{str(host).strip()}:{int(port)}".lower()


class PulsePort:
    """주소 하나(MOXA 포트 하나). 지금 붙은(점유한) 드라이버 하나만 Worker 를 갖는다."""

    def __init__(self, clock, endpoint: str, transport_factory: Callable[[str, str], Any], writes_allowed: bool,
                 on_event: Optional[Callable[[Any], None]] = None) -> None:
        self.clock, self.endpoint = clock, endpoint
        self.transport_factory = transport_factory     # (kind, chamber) → Transport
        self.writes_allowed = writes_allowed
        self.on_event = on_event
        self.drivers: Dict[Tuple[str, str], Any] = {}   # (chamber, kind) → 드라이버
        self.sources: Dict[Tuple[str, str], Any] = {}   # (chamber, kind) → snapshot_bus.Source
        self.owner: Optional[Tuple[str, str]] = None    # (chamber, kind)
        self.worker: Optional[Worker] = None
        self._lock = threading.RLock()

    def add(self, kind: str, chamber: str, driver, source=None) -> None:
        self.drivers[(chamber, kind)] = driver
        if source is not None:
            self.sources[(chamber, kind)] = source
            source.worker = None                        # 점유 전에는 off 품질

    def bind_source(self, kind: str, chamber: str, source) -> None:
        with self._lock:
            self.sources[(chamber, kind)] = source
            source.worker = self.worker if self.owner == (chamber, kind) else None

    def claim(self, kind: str, chamber: str) -> Tuple[bool, str, Optional[dict]]:
        """(성공, 거부 문구, 소유자 {"ch","kind"}). 같은 챔버·같은 종류의 다시 잡기는 성공(이미 붙은 Worker 유지)."""
        with self._lock:
            if self.owner is not None:
                if self.owner == (chamber, kind):
                    return True, "", None
                o_ch, o_kind = self.owner
                msg = (f"{KIND_NAME.get(kind, kind)} 시작 불가: 동일 주소({self.endpoint}) 장비를 "
                       f"{o_ch.upper()}({KIND_NAME.get(o_kind, o_kind)}) 공정이 사용 중입니다.")
                return False, msg, {"ch": o_ch, "kind": o_kind}
            drv = self.drivers.get((chamber, kind))
            if drv is None:
                raise KeyError(f"{chamber}.{kind} 드라이버가 이 포트({self.endpoint})에 없음")
            transport = self.transport_factory(kind, chamber)
            w = Worker(f"{chamber}.{kind}", transport, writes_allowed=self.writes_allowed,
                       on_event=self.on_event, **drv.worker_params())
            drv.bind(w)
            self.owner, self.worker = (chamber, kind), w
            src = self.sources.get((chamber, kind))
            if src is not None:
                src.worker = w
            w.start()
            return True, "", None

    def release(self, chamber: Optional[str] = None) -> bool:
        """점유 해제(chamber 를 주면 그 챔버가 가진 경우만). Worker 를 멈추고 드라이버를 떼어 낸다."""
        with self._lock:
            if self.owner is None or (chamber is not None and self.owner[0] != chamber):
                return False
            key, w = self.owner, self.worker
            self.owner, self.worker = None, None
            src = self.sources.get(key)
            if src is not None:
                src.worker = None
            drv = self.drivers.get(key)
        if drv is not None:
            drv.unbind()
        if w is not None:
            if threading.current_thread() is getattr(w, "_thread", None):
                threading.Thread(target=w.stop, name=f"pulse-release-{key[1]}", daemon=True).start()
            else:
                w.stop()
        return True


class PulsePorts:
    """주소 → PulsePort. 챔버 둘의 펄스 드라이버가 이 객체 하나를 같이 쓴다."""

    def __init__(self, clock, transport_factory: Callable[[str, str], Any], writes_allowed: bool,
                 on_event: Optional[Callable[[Any], None]] = None) -> None:
        self.clock, self.transport_factory = clock, transport_factory
        self.writes_allowed, self.on_event = writes_allowed, on_event
        self.ports: Dict[str, PulsePort] = {}

    def add(self, kind: str, chamber: str, driver, source=None) -> PulsePort:
        ep = driver.endpoint_key()
        port = self.ports.get(ep)
        if port is None:
            port = self.ports[ep] = PulsePort(self.clock, ep, self.transport_factory, self.writes_allowed, self.on_event)
        port.add(kind, chamber, driver, source)
        return port

    def port_of(self, kind: str, chamber: str) -> PulsePort:
        for p in self.ports.values():
            if (chamber, kind) in p.drivers:
                return p
        raise KeyError(f"{chamber}.{kind}")

    def claim(self, kind: str, chamber: str) -> Tuple[bool, str, Optional[dict]]:
        return self.port_of(kind, chamber).claim(kind, chamber)

    def release_chamber(self, chamber: str) -> int:
        """v1 release_pulse_endpoints(ch) — 그 챔버가 가진 점유를 모두 푼다. 푼 개수."""
        return sum(1 for p in self.ports.values() if p.release(chamber))

    def owner(self, kind: str, chamber: str) -> Optional[Tuple[str, str]]:
        return self.port_of(kind, chamber).owner


def tcp_transport_factory(clock, cfg: dict):
    """실제 MOXA NPort 전송(prod·bench). DC Pulse 는 SO_KEEPALIVE = DCP_TCP_KEEPALIVE(v1 기본 false → 0,
    dc_pulse.py:2161-2169), RF Pulse 는 설정과 무관하게 늘 1(v1 rf_pulse.py:1057-1063). 연결 제한은 종류별 v1 값."""
    import socket

    from v2.common.transport import TcpSettings, TcpTransport

    def make(kind: str, chamber: str):
        host, port = endpoint(cfg, chamber, kind)
        keep = 1 if kind == "rfpulse" else (1 if cfg.get("DCP_TCP_KEEPALIVE", False) else 0)
        tmo = float(cfg.get("RFPULSE_CONNECT_TIMEOUT_S", 1.5) if kind == "rfpulse" else cfg.get("DCP_CONNECT_TIMEOUT_S", 3.0))

        def sock_factory(s: TcpSettings):
            sk = socket.create_connection((s.host, s.port), timeout=s.connect_timeout_s)
            sk.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, keep)
            sk.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sk.setblocking(False)
            return sk
        return TcpTransport(clock, TcpSettings(host, int(port), tmo), socket_factory=sock_factory)
    return make


def chan_get(cfg: dict, chamber: str, key: str, default=None):
    """채널 구역 키(CH1_/CH2_) 먼저, 없으면 공통 키 — v1 _cfg_get(채널 모듈 → config_common) 순서."""
    ck = f"{chamber.upper()}_{key}"
    if ck in cfg:
        return cfg[ck]
    return cfg.get(key, default)


def endpoint(cfg: dict, chamber: str, kind: str) -> Tuple[str, int]:
    pre = "DCPULSE" if kind == "dcpulse" else "RFPULSE"
    host = chan_get(cfg, chamber, f"{pre}_TCP_HOST", "192.168.1.50")
    port = chan_get(cfg, chamber, f"{pre}_TCP_PORT", 4007 if kind == "dcpulse" else 4005)
    return str(host), int(port)

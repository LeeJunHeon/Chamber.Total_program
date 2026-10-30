"""챔버12 조립(사용자 지시 3장 (2)·5장 2단계) — 모든 worker·드라이버 + 1초 묶기 + 기록 + v1 기록 출력 + 엔진 꽂는 자리.

모드에 따라 전송만 다르다: sim = 시뮬레이터(sim.rig.make_sims·sim_transports), bench·prod = TCP(tcp_transports — PLC 직결,
나머지 MOXA NPort). 쓰기 허용은 모드 정책(common.mode). 시험 하니스(sim.rig.Ch12Rig)와 실행 프로그램(app)이 이 조립 하나를
쓴다(배선이 두 벌이 되지 않게). 통신층 명령 가드(cmd_guard — 1단계 남은 일)는 guard(이름, 전송) 로 전송 바로 위에 끼운다.

장비(블록 이름 = snapshot_fields.BLOCK_ORDER)
- PLC 하나(plc): CH1·CH2·플라즈마 클리닝·로봇·히터가 공유 — 출처 태그 proxy(shared.plc). 같은 1초 읽기에서 CH2 DC(ch2.dc —
  DC1, DC2 는 CH2_SUPPORTS_DC2 이고 히터가 없을 때만), CH2 RF 연속(ch2.rf), 플라즈마 클리닝 RF(pc.rf)를 해독한다.
- 챔버마다 MFC·IG(상시 연결 — D37), DC Pulse·RF Pulse(PulsePorts — CH1 4007·CH2 4005, 공정이 고를 때만 연결),
  두께 STM-100(CHn_SUPPORTS_THICKNESS 일 때만 포트를 연다), OES·RGA 링크(별도 프로세스 워커 — links=False 면 만들지 않고 off,
  sim 은 *_WORKER_FAKE 를 켠 설정으로).
- TSP(tsp — 쓰는 동안만 205 읽기, 소켓은 명령 때만)·TSP 게이지(tsp.ig — D67: shared.tsp_ig 를 잡는 동안 CH1 IG worker 를
  공유, 설정값은 CH2 값 — SharedGauge), 카메라(camera).
- 설정에서 꺼진 장비(SUPPORTS_* 등)는 worker 를 띄우지 않는다(포트를 열지 않음) — 1초 행에서 off.

1초 행(TickLoop, 정각 1초)
- tick 훅: PLC 하트비트(on_tick) → 엔진 on_tick_start(t) → 시나리오 훅.
- 행 열(extra): 프로그램 상태(prog.*) + 엔진 snapshot_fields()(공정 맥락·목표값 — snapshot_fields.EXTRA_FIELDS 에 있는 열만).
- 행이 닫히면(sink 순서): 기록(Recorder — 일별 SQLite WAL + CSV, NAS 사본은 모드 정책이 켜고 NAS_COPY_DIR 이 있을 때만)
  → 안전(4단계 — safety.on_row 자리) → 엔진들 on_row(RowView) → v1 기록 출력(outputs.row_feed) → 행 구독자(허브 — 7단계 자리).
  기록·출력은 큐에 넣기만 한다(tick 은 기다리지 않는다). 엔진·출력의 예외는 INTERNAL_ERROR 이벤트로 남기고 tick 은 계속.
엔진(3단계): register_engine(name, obj) — obj 에 on_tick_start(t)·on_row(view)·snapshot_fields()·busy 가 있으면 부른다.
내부 이벤트: 드라이버·worker·엔진(emit) → events(링 버퍼, Retention.events) → 기록(events 표) → 이벤트 구독자.
  TRANSIENT_KINDS(OES 스펙트럼 — 화면 그래프 값)는 구독자에게만(링 버퍼·기록 없이).
"""
from __future__ import annotations

import collections
import datetime as _dt
import threading
import time
from collections import ChainMap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from v2.common.config_store import ConfigStore
from v2.common.mode import RunMode, policy
from v2.common.recorder import Recorder
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop, iso_local
from v2.common.transport import TcpTransport, Transport
from v2.common.worker import Result, Status, Worker
from v2.ch12 import snapshot_fields as SF
from v2.ch12.drivers import dcpulse as DCP, ig as G, mfc as M, plc as P, rfpulse as RFP, stm100 as STM, tsp as T
from v2.ch12.drivers.cache_source import CacheWorker
from v2.ch12.drivers.camera import CameraDriver
from v2.ch12.drivers.dc import DcPower
from v2.ch12.drivers.oes_link import OesLink
from v2.ch12.drivers.pulse_port import PulsePorts, chan_get, tcp_transport_factory
from v2.ch12.drivers.rf import RfPower
from v2.ch12.drivers.rga_link import RgaLink
from v2.ch12.outputs.row_feed import RowFeed
from v2.ch12.shared import SharedDevices, stop_worker

CHAMBERS = SF.CHAMBERS
EVENTS_KEEP = 20000
PULSE_KINDS = (("dcpulse", DCP.DcPulseDriver, "SUPPORTS_DCPULSE"), ("rfpulse", RFP.RfPulseDriver, "SUPPORTS_RFPULSE"))
WARN_KINDS = frozenset({"disconnected", "connect_failed", "long_outage", "timeout", "link_down", "heartbeat_fail",
                        "command_failed", "target_failed", "power_off_failed"})   # 사유 코드가 없어도 warn 으로 기록
SIM_LINK_CFG = {"OES_WORKER_FAKE": True, "RGA_WORKER_FAKE": True}     # sim: 워커는 가짜 DLL·가짜 RGA 로(실제 프로세스)
TRANSIENT_KINDS = frozenset({"spectrum"})   # 화면 전용(OES 스펙트럼 0.5 s) — 링 버퍼·기록(events 표)에 넣지 않고 구독자(허브)에게만


@dataclass(frozen=True)
class Retention:
    """메모리에 두는 고정 크기 기록의 크기 — 한 곳(챔버K system.Retention 과 같은 뜻). 운전 기록은 Recorder·출력 파일이 남기므로
    이 기록은 화면·시험용이다. 시험 하니스는 결과를 보려고 크게, 실행 프로그램(APP_RETENTION)은 작게.
    크기가 상수로 정해지는 다른 기록: recorder.MAX_QUEUE 20000·ERRORS_KEEP 200, outputs.base MAX_QUEUE, simdev.SIM_LOG_KEEP,
    TickLoop STATS_KEEP. 엔진의 events 는 register_engine 때 이 크기로 바꾼다(속성 events 가 deque 면)."""
    events: int = EVENTS_KEEP          # Ch12System.events·엔진 events
    tick_stats: int = 21600            # TickLoop.jitters·close_lags(6 시간분)
    sim_log: Optional[int] = None      # 시뮬레이터가 받은 요청 기록(None = simdev.SIM_LOG_KEEP)


TEST_RETENTION = Retention()
APP_RETENTION = Retention(events=1000, tick_stats=21600, sim_log=1000)


@dataclass
class Logged:
    """내부 이벤트 한 건(링 버퍼·구독자). event 는 원래 객체(드라이버 이벤트 dataclass 등)."""
    t_wall: float
    source: str
    kind: str
    detail: str = ""
    reason: Optional[str] = None
    code: Optional[str] = None
    level: str = "info"
    event: Any = None


@dataclass
class TransportSet:
    """장비별 전송. fixed: 상시 장비(plc 는 안쪽 전송 — RetryOpenTransport 는 조립이 씌움), pulse: (종류, 챔버) → 새 전송
    (점유할 때마다), tsp_ig: () → 새 전송(쓰지 않음 — TSP 게이지는 CH1 IG worker 공유, D67. 시험대 호환으로 남김)."""
    fixed: Dict[str, Transport]
    pulse: Callable[[str, str], Transport]
    tsp_ig: Callable[[], Transport]
    extra: Dict[str, Any] = field(default_factory=dict)


class SwapSource(Source):
    """worker 를 바꿔 끼우는 1초 소스(펄스 점유·TSP 게이지 — 잡을 때 붙이고 놓을 때 None). 떼어 낼 때 그 worker 큐에 남은
    1초 읽기 요청을 CANCELLED 로 끝낸다 — Worker.stop 은 큐에 남은 요청의 on_done 을 부르지 않아서, 놓는 순간 줄 서 있던 읽기가
    묶기(snapshot_bus)에 '진행 중'으로 남으면 그 뒤 모든 행이 마감까지 기다리고(행 닫힘 950 ms) 다시 잡아도 그 소스를 읽지 않는다."""

    @property
    def worker(self):
        return self.__dict__.get("_w")

    @worker.setter
    def worker(self, w) -> None:
        old = self.__dict__.get("_w")
        self.__dict__["_w"] = w
        if old is not None and old is not w:
            left = old.queue.drain(lambda r: not str(getattr(r, "name", "")).startswith("read:"))
            now = old.clock.monotonic()
            for req in left:
                if req.on_done:
                    req.on_done(Result(req, Status.CANCELLED, reason="CANCELLED", started=now, finished=now))


class SharedGauge:
    """TSP 게이지(D67) — CH1 IG worker(4001)를 공유 소유권으로 잡는다(두 번째 연결을 열지 않음 — CH1 IG 는 상시 연결, D37).
    shared.OnDemand 와 같은 모양(acquire·release·owner·driver·worker·source·source_read)이라 SharedDevices.tsp_ig 로 그대로 쓴다.
    잡으면: 같은 worker 위에 chamber="tsp" IgDriver(설정은 CH2 값 — v1 TSP 런타임의 cfg 없는 AsyncIG, D189 이상 동작 그대로)를
    만들고, tsp.ig 1초 소스를 그 worker 에 붙이고, ch1.ig 1초 읽기를 TSP 드라이버의 RDI 로 바꾼다(그동안 ch1.ig 열은 TSP 쪽 값 —
    한 tick 에 RDI 하나, tsp.ig 는 같은 tick 의 그 값을 싣는다). IG 명령 간격(ready_at)은 두 드라이버 사이에 넘겨준다.
    놓으면 되돌린다(worker 는 멈추지 않는다). 한 번에 한 소유자(같은 소유자 다시 잡기는 지금 것 유지)."""

    name = "tsp.ig"

    def __init__(self, clock, cfg, writes_allowed: bool, ig1, worker: Worker, ch1_source: Callable[[], Source],
                 source: Source, on_event) -> None:
        self.clock, self.cfg, self.writes_allowed = clock, cfg, writes_allowed
        self.ig1, self._w, self._ch1_source, self.source, self.on_event = ig1, worker, ch1_source, source, on_event
        self.owner: Optional[str] = None
        self.driver: Optional[G.IgDriver] = None
        self.worker: Optional[Worker] = None
        self._prev_read = None
        self._prev_connect = None
        self._tick: Optional[tuple] = None          # (tick seq, 이번 tick ch1 쪽 RDI 예외 또는 None)
        self._lock = threading.RLock()
        source.worker = None
        source.read = self.source_read

    def acquire(self, owner: str):
        with self._lock:
            if self.owner is not None:
                return (True, owner) if self.owner == owner else (False, self.owner)
            w, ig1 = self._w, self.ig1
            prev = w.on_connect
            drv = G.IgDriver(self.clock, w, self.cfg, self.writes_allowed, on_event=self.on_event, chamber="tsp")
            self._prev_connect = prev                                  # IgDriver 가 on_connect 를 자기 것으로 바꾼다 — 둘 다 부름

            def on_connect(session, a=prev, b=drv._on_connect):
                if a is not None:
                    a(session)
                b(session)
            w.on_connect = on_connect
            drv.connected_at = ig1.connected_at
            drv.ready_at = max(drv.ready_at, ig1.ready_at)             # 직전 CH1 RDI 뒤 IG_GAP_MS 를 지킨다
            src1 = self._ch1_source()
            self._prev_read, src1.read = src1.read, self._read_ch1
            self.driver, self.worker, self.owner = drv, w, owner
            self._tick = None
            self.source.worker = w
            return True, owner

    def release(self, owner: Optional[str] = None) -> bool:
        with self._lock:
            if self.owner is None or (owner is not None and self.owner != owner):
                return False
            drv, w = self.driver, self.worker
            self.owner, self.worker, self.driver = None, None, None
            self.source.worker = None
            self._ch1_source().read = self._prev_read
            w.on_connect = self._prev_connect
            ig1 = self.ig1
            ig1.ready_at = max(ig1.ready_at, drv.ready_at)
            if drv.latest is not None and (ig1.latest is None or drv.latest[0] >= ig1.latest[0]):
                ig1.latest = drv.latest
        # 큐에 남은 tsp.ig 1초 읽기만 CANCELLED 로 끝낸다(ch1.ig 읽기는 그대로 — SwapSource 와 같은 이유)
        left = w.queue.drain(lambda r: str(getattr(r, "name", "")) != "read:tsp.ig")
        now = w.clock.monotonic()
        for req in left:
            if req.on_done:
                req.on_done(Result(req, Status.CANCELLED, reason="CANCELLED", started=now, finished=now))
        return True

    def _read_ch1(self, session, seq: int) -> Dict[str, Any]:
        drv = self.driver
        if drv is None:                                                # 막 놓음
            return self.ig1.source_read(session, seq)
        try:
            got = drv.source_read(session, seq)
        except Exception as e:
            self._tick = (seq, e)
            raise
        self._tick = (seq, None)
        return {"ch1.ig." + k[len("tsp.ig."):]: v for k, v in got.items()}

    def source_read(self, session, seq: int) -> Dict[str, Any]:
        drv = self.driver
        if drv is None:
            raise RuntimeError(f"{self.name}: 잡지 않음")
        t = self._tick
        if t is not None and t[0] == seq and t[1] is not None:
            raise t[1]
        return drv._fields(self.clock.monotonic())


def tcp_transports(clock, cfg: Dict[str, Any]) -> TransportSet:
    """bench·prod — PLC 직결 Modbus TCP, 나머지 MOXA NPort TCP. 만들기만 하고 열지 않는다(worker 가 연다)."""
    from v2.common.transport import TcpSettings
    fixed: Dict[str, Transport] = {
        "plc": TcpTransport(clock, TcpSettings(str(cfg["PLC_TCP_HOST"]), int(cfg["PLC_TCP_PORT"]), float(cfg["PLC_TIMEOUT_S"]))),
        "tsp": T.make_transport(clock, cfg)}
    for ch in CHAMBERS:
        fixed[f"{ch}.mfc"] = TcpTransport(clock, M.tcp_settings(cfg, ch))
        fixed[f"{ch}.ig"] = TcpTransport(clock, G.tcp_settings(cfg, ch))
        fixed[f"{ch}.stm100"] = STM.make_transport(clock, cfg, ch)
    return TransportSet(fixed, tcp_transport_factory(clock, cfg), lambda: TcpTransport(clock, G.tcp_settings(cfg, "tsp")))


def supports(cfg, chamber: str, key: str) -> bool:
    return bool(chan_get(cfg, chamber, key, False))


class Ch12System:
    def __init__(self, clock, cfg: Dict[str, Any], data_dir: Path, transports: TransportSet, mode: str = "sim",
                 store: Optional[ConfigStore] = None, retention: Retention = TEST_RETENTION, nas_dir: Optional[str] = None,
                 links: bool = True, camera_backend=None, guard: Optional[Callable[[str, Transport], Transport]] = None,
                 scenario_hook: Optional[Callable[[Any], None]] = None, program: str = "ch12-v2",
                 version: str = "unknown") -> None:
        self.clock, self.cfg, self.mode = clock, cfg, mode         # cfg: 모든 구성 요소가 같이 보는 사전(복사하지 않음)
        self.policy = policy(RunMode(mode))
        self.program, self.version = program, version
        self.retention = rt = retention
        self.store = store
        if store is not None and not store.values:
            store.load()
        self.events: "collections.deque[Logged]" = collections.deque(maxlen=rt.events)
        self.event_listeners: List[Callable[[Logged], None]] = []
        self._ev_sources: set = set()                                       # _ev_for 로 만든 이벤트 길의 source 이름
        self.device_sources: frozenset = frozenset()   # 그중 장비(드라이버·worker·링크) 것 — 장비를 다 만든 뒤 고정(DEBUG_PRINT 콘솔 줄, app)
        self.row_listeners: List[Callable[[Dict[str, Any]], None]] = []    # 허브(7단계)·시험 — 기록 출력 뒤
        self.engines: Dict[str, Any] = {}
        self.safety = None                                                   # 4단계(안전 규칙) — on_row(view)
        self.internal_errors = 0
        self.unknown_fields: set = set()
        self.recorder: Optional[Recorder] = None
        self._t0 = clock.monotonic()
        c, wr = cfg, self.policy.device_writes
        g = guard or (lambda name, tr: tr)
        fx = transports.fixed
        self.workers: Dict[str, Worker] = {}     # 상시 worker(이름 → Worker). 펄스·TSP 게이지는 잡을 때만(pulse·shared)
        self.enabled: Dict[str, bool] = {}       # 상시 worker 를 띄우는가

        # ── PLC(공유) + PLC 해독 블록 ──
        ptr = P.make_transport(clock, c, g("plc", fx["plc"]))
        self.workers["plc"] = P.make_worker(ptr, c, wr, on_event=self._ev_for("plc"))
        diag = None if mode == "sim" else _probe
        self.plc = P.PlcDriver(clock, self.workers["plc"], c, wr, on_event=self._ev_for("plc"), diag_probe=diag)
        self.dc: Dict[int, Optional[DcPower]] = {1: None, 2: None}
        if supports(c, "ch2", "SUPPORTS_DC"):
            self.dc[1] = DcPower(clock, self.plc, c, wr, self._ev_for("ch2.dc"), chamber="ch2", unit=1)
            # DC2 는 CH2_SUPPORTS_DC2 이고 히터가 없을 때만(D/A CH3 = 히터 출력 — DcPower.allowed 와 같은 조건)
            if bool(c.get("CH2_SUPPORTS_DC2", False)) and not bool(c.get("CH2_SUPPORTS_HEATER", False)):
                self.dc[2] = DcPower(clock, self.plc, c, wr, self._ev_for("ch2.dc"), chamber="ch2", unit=2)
        self.rf: Dict[str, Optional[RfPower]] = {"ch2": None, "pc": None}
        if supports(c, "ch2", "SUPPORTS_RF_CONT"):
            self.rf["ch2"] = RfPower(clock, self.plc, c, wr, self._ev_for("ch2.rf"), chamber="ch2", mode="ch2")
        self.rf["pc"] = RfPower(clock, self.plc, c, wr, self._ev_for("pc.rf"), chamber="pc", mode="pc")
        # CH2 히터 PLC 해독기(CH2_SUPPORTS_HEATER) — PLC 1초 소스 블록(ch2.heater)에 들어가게 소스보다 먼저. 엔진은 조립(assembly)
        self.heater_drv = None
        if bool(c.get("CH2_SUPPORTS_HEATER", False)):
            from v2.ch12.drivers.heater import HeaterDriver
            self.heater_drv = HeaterDriver(clock, self.plc, c, wr, on_event=self._ev_for("ch2.heater"), chamber="ch2")
        sources: List[Source] = [Source("plc", self.workers["plc"], self.plc.source_read, self.plc.source_blocks(),
                                        int(c["TICK_DEADLINE_MS"]))]

        # ── 챔버마다 MFC·IG(상시), 두께(켜진 것만) ──
        self.mfc: Dict[str, M.MfcDriver] = {}
        self.ig: Dict[str, G.IgDriver] = {}
        self.stm: Dict[str, STM.Stm100Driver] = {}
        for ch in CHAMBERS:
            w = self.workers[f"{ch}.mfc"] = M.make_worker(clock, g(f"{ch}.mfc", fx[f"{ch}.mfc"]), c, ch, wr,
                                                          on_event=self._ev_for(f"{ch}.mfc"))
            self.mfc[ch] = M.MfcDriver(clock, w, c, wr, on_event=self._ev_for(f"{ch}.mfc"), chamber=ch)
            sources.append(Source(f"{ch}.mfc", w, self.mfc[ch].source_read, (f"{ch}.mfc",), M.source_deadline_ms(c)))
            w = self.workers[f"{ch}.ig"] = G.make_worker(clock, g(f"{ch}.ig", fx[f"{ch}.ig"]), c, ch, wr,
                                                         on_event=self._ev_for(f"{ch}.ig"))
            self.ig[ch] = G.IgDriver(clock, w, c, wr, on_event=self._ev_for(f"{ch}.ig"), chamber=ch)
            sources.append(Source(f"{ch}.ig", w, self.ig[ch].source_read, (f"{ch}.ig",), G.source_deadline_ms(c)))
            w = self.workers[f"{ch}.stm100"] = Worker(f"{ch}.stm100", g(f"{ch}.stm100", fx[f"{ch}.stm100"]),
                                                      writes_allowed=wr, on_event=self._ev_for(f"{ch}.stm100"),
                                                      **STM.worker_kwargs())
            self.stm[ch] = STM.Stm100Driver(clock, w, c, wr, on_event=self._ev_for(f"{ch}.stm100"), chamber=ch)
            sources.append(self.stm[ch].source())
        for k in self.workers:
            self.enabled[k] = not k.endswith(".stm100") or self.stm[k.split(".")[0]].enabled

        # ── 펄스 전원(챔버마다 DC Pulse·RF Pulse — 공정이 고를 때만 연결) ──
        self.pulse = PulsePorts(clock, lambda kind, ch: g(f"{ch}.{kind}", transports.pulse(kind, ch)), wr,
                                on_event=self._ev_for("pulse"))
        self.dcpulse: Dict[str, Optional[DCP.DcPulseDriver]] = {}
        self.rfpulse: Dict[str, Optional[RFP.RfPulseDriver]] = {}
        for ch in CHAMBERS:
            for kind, cls, key in PULSE_KINDS:
                name = f"{ch}.{kind}"
                src = SwapSource(name, None, lambda s, n: {}, (name,), int(c["TICK_DEADLINE_MS"]))
                drv = None
                if supports(c, ch, key):
                    drv = cls(clock, None, c, wr, on_event=self._ev_for(name), chamber=ch)
                    src.read = drv.source_read
                    self.pulse.add(kind, ch, drv, src)
                else:
                    src.enabled = False
                getattr(self, kind)[ch] = drv
                sources.append(src)

        # ── TSP(쓰는 동안만 읽기)·TSP 게이지(잡을 때만 worker)·카메라 ──
        w = self.workers["tsp"] = Worker("tsp", g("tsp", fx["tsp"]), writes_allowed=wr, on_event=self._ev_for("tsp"))
        self.enabled["tsp"] = True
        self.tsp = T.TspDriver(clock, w, c, wr, on_event=self._ev_for("tsp"))
        sources.append(self.tsp.source())
        # TSP 게이지(D67): 두 번째 연결을 열지 않고 CH1 IG worker(4001)를 공유 소유권으로 잡는다 — transports.tsp_ig 는 쓰지 않음
        ig_src = Source("tsp.ig", None, lambda s, n: {}, ("tsp.ig",), G.source_deadline_ms(c))
        self.tsp_ig = SharedGauge(clock, c, wr, self.ig["ch1"], self.workers["ch1.ig"],
                                  lambda: self.sources["ch1.ig"], ig_src, self._ev_for("tsp.ig"))
        sources.append(ig_src)
        self.camera = CameraDriver(clock, c, on_event=self._ev_for("camera"), backend=camera_backend, writes_allowed=wr)
        sources.append(self.camera.source())

        # ── OES·RGA 링크(별도 프로세스 워커 — 메인은 상태만 1초 행에) ──
        lcfg = ChainMap(dict(SIM_LINK_CFG), c) if mode == "sim" else c
        self.oes: Dict[str, Optional[OesLink]] = {}
        self.rga: Dict[str, Optional[RgaLink]] = {}
        for ch in CHAMBERS:
            for kind, cls, store_ in (("oes", OesLink, self.oes), ("rga", RgaLink, self.rga)):
                name = f"{ch}.{kind}"
                link = cls(clock, None, lcfg, wr, on_event=self._ev_for(name), chamber=ch) if links else None
                store_[ch] = link
                sources.append(Source(name, CacheWorker(clock, name=name) if link else None,
                                      link.source_read if link else (lambda s, n: {}), (name,), 900, enabled=bool(link)))

        self.device_sources = frozenset(self._ev_sources)      # 여기까지가 장비 — 엔진(조립)의 _ev_for 는 들어가지 않는다

        # ── 블록을 채우는 소스가 없으면(설정에서 꺼진 PLC 해독 블록) off 소스 ──
        have = {b for s in sources for b in s.blocks}
        for b in SF.BLOCK_ORDER:
            if b not in have:
                sources.append(Source(f"off:{b}", None, lambda s, n: {}, (b,), 900, enabled=False))
        self.sources = {s.name: s for s in sources}
        self.shared = SharedDevices(self.plc, self.camera, self.pulse, self.mfc, tsp_ig=self.tsp_ig)

        # ── 묶기·기록·출력·tick ──
        self.bus = SnapshotBus(clock, SF.device_blocks(), sources, int(c["TICK_DEADLINE_MS"]), SF.EXTRA_FIELDS)
        nas = nas_dir if (self.policy.nas_copy and nas_dir) else None
        self.recorder = Recorder(Path(data_dir), self.bus.columns, retention_days=int(c["SNAPSHOT_RETENTION_DAYS"]),
                                 nas_dir=Path(nas) if nas else None, nas_period_s=float(c["NAS_SYNC_PERIOD_SEC"]),
                                 nas_today_period_s=float(c["NAS_TODAY_COPY_SEC"]),
                                 today_fn=lambda: _dt.date.fromtimestamp(clock.wall()), wall_fn=clock.wall)
        self.outputs = RowFeed(c, clock, plc_connected=lambda: self.workers["plc"].connected,
                               thickness={ch: d for ch, d in self.stm.items() if d.enabled},
                               log=lambda src, msg: self.emit(src, "log", msg),
                               on_selfwatch=lambda kind, ch, text: self.emit("selfwatch", "selfwatch", text,
                                                                             level="warn", code="SELFWATCH"))
        self._scenario_hook = scenario_hook
        self.loop = TickLoop(clock, self.bus, [self.recorder.put_row, self._safety_row, self._on_row],
                             tick_hooks=[self._tick], extra_fn=self._extra)
        for name in ("jitters", "close_lags"):
            setattr(self.loop, name, collections.deque(maxlen=rt.tick_stats))
        self.started = False

    # ───────── 엔진 꽂는 자리 ─────────
    def register_engine(self, name: str, obj: Any) -> None:
        """엔진(챔버별 공정·플라즈마 클리닝·TSP·히터·두께 — 3단계)을 꽂는다. 있는 것만 부른다:
        on_tick_start(tick) — 읽기를 보낸 직후, snapshot_fields() → {열: 값} — 이번 행의 공정 맥락·목표값,
        on_row(view) — 행이 닫힌 뒤(안전 다음, 기록 출력 앞), busy — 설정 적용·종료 판단."""
        if name in self.engines:
            raise ValueError(f"엔진 {name} 이 이미 있음")
        q = getattr(obj, "events", None)
        if isinstance(q, collections.deque) and q.maxlen != self.retention.events:
            obj.events = collections.deque(q, maxlen=self.retention.events)
        self.engines[name] = obj

    def unregister_engine(self, name: str) -> Any:
        return self.engines.pop(name, None)

    def engine(self, name: str) -> Any:
        return self.engines.get(name)

    def busy(self) -> bool:
        """엔진 하나라도 busy(공정·정지 순서·히터 레시피 …)면 True — 설정 idle 키 대기·종료 거부가 본다."""
        return any(bool(getattr(e, "busy", False)) for e in self.engines.values())

    def _call(self, where: str, fn, *a):
        try:
            return fn(*a)
        except Exception as e:                    # 엔진 하나의 오류가 tick·기록을 멈추지 않는다 — 기록에 남긴다
            self.internal_errors += 1
            self.emit(where, "internal_error", repr(e), level="error", code="INTERNAL_ERROR")
            return None

    # ───────── 1초 tick ─────────
    def _tick(self, t) -> None:
        self.plc.on_tick(t)
        for n, e in list(self.engines.items()):
            f = getattr(e, "on_tick_start", None)
            if f is not None:
                self._call(f"engine:{n}", f, t)
        if self._scenario_hook is not None:
            self._call("scenario", self._scenario_hook, t)

    def _extra(self, t) -> Dict[str, Any]:
        out = SF.program_state()
        out.update({"prog.uptime_s": round(self.clock.monotonic() - self._t0, 1),
                    "prog.rec_pending": self.recorder._q.qsize(), "prog.rec_dropped": self.recorder.dropped,
                    "prog.out_pending": self.outputs.pending, "prog.events": len(self.events),
                    "prog.engines": ",".join(self.engines) or None})
        for n, e in list(self.engines.items()):
            f = getattr(e, "snapshot_fields", None)
            if f is None:
                continue
            got = self._call(f"engine:{n}", f) or {}
            for k, v in got.items():
                if k in SF.ENGINE_COLUMNS:
                    out[k] = v
                else:
                    self.unknown_fields.add(k)
        return out

    def _safety_row(self, row) -> None:
        if self.safety is not None:
            self._call("safety", self.safety.on_row, SF.RowView(row))

    def _on_row(self, row) -> None:
        view = SF.RowView(row)
        for n, e in list(self.engines.items()):
            f = getattr(e, "on_row", None)
            if f is not None:
                self._call(f"engine:{n}", f, view)
        self._call("outputs", self.outputs.on_row, row)
        for f in list(self.row_listeners):
            self._call("row_listener", f, row)

    # ───────── 내부 이벤트 ─────────
    def _ev_for(self, source: str) -> Callable[[Any], None]:
        self._ev_sources.add(source)

        def on(e) -> None:
            kind = str(getattr(e, "kind", type(e).__name__))
            reason = getattr(e, "reason", None)
            code = getattr(e, "code", None)
            level = getattr(e, "level", None) or ("warn" if (reason or code or kind in WARN_KINDS) else "info")
            src = getattr(e, "device", None) or source
            self._log(Logged(self.clock.wall(), str(src), kind, str(getattr(e, "detail", "") or ""), reason, code,
                             str(level), e))
        return on

    def emit(self, source: str, kind: str, detail: str = "", level: str = "info", code: Optional[str] = None,
             reason: Optional[str] = None, event: Any = None) -> None:
        """엔진·조립이 내는 이벤트(드라이버 이벤트와 같은 길)."""
        self._log(Logged(self.clock.wall(), source, kind, detail, reason, code, level, event))

    def _log(self, e: Logged) -> None:
        if e.kind in TRANSIENT_KINDS:                 # 화면 그래프 값 — 허브가 state 에 마지막 것만 둔다
            for f in list(self.event_listeners):
                try:
                    f(e)
                except Exception:
                    self.internal_errors += 1
            return
        self.events.append(e)
        if self.recorder is not None:
            self.recorder.put_event(iso_local(e.t_wall), e.level, (e.code or e.reason or e.kind).upper(), e.source,
                                    e.detail)
        for f in list(self.event_listeners):
            try:
                f(e)
            except Exception:
                self.internal_errors += 1

    # ───────── 생명주기 ─────────
    def start(self, ready_timeout_s: float = 15.0) -> bool:
        """기록·출력 시작 → 켜진 장비 worker 시작 → 모두 연결·초기화될 때까지(최대 ready_timeout_s) 기다림.
        모두 준비되면 True(아니어도 worker 는 계속 재연결). tick 은 부른 쪽이 loop.run 으로(실행 프로그램은 스레드)."""
        self.recorder.start()
        self.outputs.start()
        with self.clock.hold():
            for k, w in self.workers.items():
                if self.enabled[k]:
                    w.start()
        for d in self.stm.values():
            d.start()
        self.started = True
        need = [k for k in self.workers if self.enabled[k] and not k.endswith(".stm100")]
        return self.clock.wait_until(self.clock.monotonic() + ready_timeout_s,
                                     lambda: all(self.workers[k].ready for k in need))

    def stop(self, drain_s: float = 2.0) -> None:
        """tick 멈춤 → 엔진 close → 펄스·TSP 게이지 놓기 → 카메라·두께 멈춤 → worker 정지(포트 닫기) → 출력·기록 flush.
        PLC 는 하트비트를 먼저 쉬고 남은 요청이 끝나기를 잠깐(drain_s) 기다린다 — 정지가 취소한 하트비트를 드라이버가
        링크 끊김으로 세어 종료 때 'PLC 연결 끊김' 알림이 나가지 않게."""
        self.loop.stop()
        for n, e in list(self.engines.items()):
            f = getattr(e, "close", None)
            if f is not None:
                self._call(f"engine:{n}", f)
        for ch in CHAMBERS:
            self.pulse.release_chamber(ch)
        self.tsp_ig.release()
        self.camera.stop()
        self.camera.join(3.0)
        for d in self.stm.values():
            d.stop()
        pw = self.workers["plc"]
        self.plc.pause_watchdog()
        if self.started and pw.connected:
            self.clock.wait_until(self.clock.monotonic() + drain_s, lambda: not pw.busy and len(pw.queue) == 0)
        for k, w in self.workers.items():
            if self.enabled[k]:
                stop_worker(w)
        if self.enabled.get("plc", True):
            self.plc.on_closed()
        for link in list(self.oes.values()) + list(self.rga.values()):
            if link is not None:
                self._call("links", link.close)
        self.outputs.stop()
        self.recorder.stop()
        self.started = False

    # ───────── 보고·시험 ─────────
    def device_table(self) -> Dict[str, str]:
        """블록 → 상태(on: 상시 worker / on-demand: 잡을 때만 / off: 꺼짐)."""
        out = {}
        for b in SF.BLOCK_ORDER:
            src = next(s for s in self.sources.values() if b in s.blocks)
            if b in ("tsp", "tsp.ig") or (b.endswith("pulse") and src.enabled):
                out[b] = "on-demand"
            elif not src.enabled:
                out[b] = "off"
            else:
                out[b] = "on"
        return out

    def retention_report(self) -> Dict[str, Optional[int]]:
        lp = self.loop
        rep = {"system.events": self.events.maxlen, "tick.jitters": lp.jitters.maxlen, "tick.close_lags": lp.close_lags.maxlen,
               "recorder.errors": self.recorder.errors.maxlen, "outputs.errors": self.outputs.writer.errors.maxlen}
        for n, e in self.engines.items():
            q = getattr(e, "events", None)
            if isinstance(q, collections.deque):
                rep[f"{n}.events"] = q.maxlen
        return rep

    def tick_stats(self) -> Dict[str, Any]:
        j, c = sorted(self.loop.jitters), sorted(self.loop.close_lags)

        def pct(xs, p):
            return round(xs[min(len(xs) - 1, int(p * len(xs)))], 1) if xs else None
        return {"rows": self.loop.rows, "missed": self.loop.missed, "jitter_ms_p50": pct(j, 0.5),
                "jitter_ms_p99": pct(j, 0.99), "jitter_ms_max": pct(j, 1.0), "close_ms_p50": pct(c, 0.5),
                "close_ms_p99": pct(c, 0.99), "close_ms_max": pct(c, 1.0)}


def _probe(host: str, port: int, timeout_s: float) -> str:
    from v2.ch12.drivers.plc_io import probe_connect
    return probe_connect(host, port, timeout_s)


def param_schema_hash() -> str:
    from v2.ch12.params import table
    return table().schema_hash()


def new_store(directory: Path) -> ConfigStore:
    from v2.ch12.params import CROSS_RULES, PARAMS
    return ConfigStore(Path(directory), PARAMS, CROSS_RULES, param_schema_hash())


def wall_iso(t: Optional[float] = None) -> str:
    return iso_local(time.time() if t is None else t)

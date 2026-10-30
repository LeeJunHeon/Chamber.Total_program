"""CH2 히터 시험대 — 두 가지(가상 시계).

FastHeaterRig : 프로세스 안 PLC 흉내(LadderPlc — plc_ladder.HeaterLadder 메모리 + 즉시 실행 worker)로 HeaterDriver·HeaterEngine 을
                돌린다. v1 tests/heater_ladder_sim.py(make·run) 와 같은 쓰임 — 1초마다 래더 스캔(1 s) → 시계 +1 → 1초 묶음 읽기
                (해독기) → on_tick_start(워치독) → 폴링 1회(v1 하니스처럼 매초, every_second=False 면 엔진 주기 on_row).
                쓰기마다 래더 dt=0 스캔(v1 하니스와 같음).
                통신 바이트는 없다 — 판정·순서·문구 시험과 긴 열 시나리오(수천 초)용.
HeaterRig     : 실제 PLC 스택(PlcSim Modbus TCP + SimTransport + v1 정책 worker + PlcDriver + SnapshotBus/TickLoop 스레드) 위에서
                같은 드라이버·엔진을 돈다 — 1초 묶음 읽기·worker 요청·워치독·재기동을 바이트 경로까지 실제로.
LadderPlc 고장 주입: connected, fail_writes, reject_regs, fail_reads(n), forced(래더 뒤 덮어쓸 D 값), on_write(가로채기).
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from v2.common.worker import Request, Result, Status
from v2.ch12.drivers import heater as H
from v2.ch12.drivers.plc import PlcPoll
from v2.ch12.heater_engine import HeaterEngine
from v2.ch12.heater_profile import HeaterCfg, cfg_to_dict
from v2.ch12.sim.plc_ladder import VAC_ITL_COIL, HeaterLadder


class FakeClock:
    def __init__(self, t: float = 1000.0, wall0: float = 1_760_000_000.0) -> None:
        self.t = float(t)
        self.wall0 = wall0

    def monotonic(self) -> float:
        return self.t

    def wall(self) -> float:
        return self.wall0 + self.t

    def __call__(self) -> float:
        return self.t


class LadderPlc:
    """PlcDriver 가운데 히터 드라이버가 쓰는 것만(connected·allow_writes·register_decoder·submit·io_write_*)."""

    def __init__(self, clock: FakeClock, gain: float = 2250.0) -> None:
        self.clock = clock
        self.D: List[int] = [0] * 512
        self.M: List[bool] = [False] * 4096
        self.ladder = HeaterLadder(self.D, self.M, gain=gain)
        self.M[VAC_ITL_COIL] = True
        self.connected = True
        self.fail_writes = False
        self.reject_regs: set = set()
        self.fail_reads = 0
        self.forced: Dict[int, int] = {}
        self.on_write: Optional[Callable[[str, int, Any], bool]] = None
        self.calls: List[tuple] = []
        self.allowed_coils: set = set()
        self.allowed_regs: set = set()
        self.decoders: List[tuple] = []
        self.ladder.scan(0.0)

    # ── 래더 값 ──
    @property
    def T(self) -> float:
        return self.ladder.T

    @T.setter
    def T(self, v: float) -> None:
        self.ladder.T = float(v)

    def _lad(name):          # noqa: N805 — 래더 속성 그대로 보이기
        return property(lambda self: getattr(self.ladder, name), lambda self, v: setattr(self.ladder, name, v))

    gain = _lad("gain")
    tc_offset = _lad("tc_offset")
    gas_loss = _lad("gas_loss")
    plasma_heat = _lad("plasma_heat")
    water_ok = _lad("water_ok")
    rst_self_reset = _lad("rst_self_reset")
    tc_fail = _lad("tc_fail")

    @property
    def vac_ok(self) -> bool:
        return bool(self.M[VAC_ITL_COIL])

    @vac_ok.setter
    def vac_ok(self, v: bool) -> None:
        self.M[VAC_ITL_COIL] = bool(v)

    def scan(self, dt: float = 1.0) -> None:
        self.ladder.scan(dt)
        for a, v in self.forced.items():
            self.D[a] = int(v) & 0xFFFF

    def restart(self, keep_temp: bool = True) -> None:
        """PLC 재기동 — 메모리 초기화(래더 T 는 그대로 둘 수 있음)."""
        t = self.ladder.T
        for i in range(len(self.D)):
            self.D[i] = 0
        for i in range(len(self.M)):
            self.M[i] = False
        self.ladder = HeaterLadder(self.D, self.M, gain=self.ladder.gain, tau=self.ladder.tau)
        if keep_temp:
            self.ladder.T = t
        self.M[VAC_ITL_COIL] = True
        self.ladder.scan(0.0)

    # ── PlcDriver API ──
    def allow_writes(self, coils=(), regs=()) -> None:
        self.allowed_coils.update(int(a) for a in coils)
        self.allowed_regs.update(int(a) for a in regs)

    def register_decoder(self, blocks, decode, on_fail=None) -> None:
        self.decoders.append((tuple(blocks), decode, on_fail))

    def submit(self, name: str, fn: Callable, *, kind: str = "write", source: str = "", on_done=None,
               purge: bool = False, not_before=None) -> Request:
        req = Request(kind, fn, name=f"plc:{source}:{name}")
        if not self.connected:
            res = Result(req, Status.LINK_DOWN, reason="LINK_DOWN")
        else:
            res = Result(req, Status.FAIL)
            try:
                res.value = fn(self)
                res.status = Status.OK
            except Exception as e:
                res.status, res.error, res.reason = Status.FAIL, repr(e), "DEVICE_REJECTED"
        if on_done is not None:
            on_done(res)
        return req

    def io_write_reg(self, s, addr: int, value: int) -> None:
        self.calls.append(("wr", int(addr), int(value)))
        if self.on_write is not None and self.on_write("wr", int(addr), int(value)):
            return
        if not self.connected:
            raise RuntimeError("E401 PLC 연결 오류")
        if self.fail_writes or int(addr) in self.reject_regs:
            raise RuntimeError("E403 write rejected")
        if int(addr) not in self.allowed_regs:
            raise RuntimeError(f"레지스터 D{addr} 은 쓰기 허용 목록 밖")
        self.D[int(addr)] = int(value) & 0xFFFF
        self.scan(0.0)

    def io_write_coil(self, s, addr: int, on: bool) -> None:
        self.calls.append(("wc", int(addr), bool(on)))
        if self.on_write is not None and self.on_write("wc", int(addr), bool(on)):
            return
        if not self.connected:
            raise RuntimeError("E401 PLC 연결 오류")
        if self.fail_writes:
            raise RuntimeError("E403 write rejected")
        if int(addr) not in self.allowed_coils:
            raise RuntimeError(f"코일 {addr} 은 쓰기 허용 목록 밖")
        self.M[int(addr)] = bool(on)
        self.scan(0.0)

    def writes(self) -> List[tuple]:
        return [c for c in self.calls if c[0] in ("wr", "wc")]

    # ── 1초 묶음 읽기(plc.py source_read 의 히터 부분) ──
    def read(self) -> None:
        if not self.connected:
            return                           # worker 가 연결 안 됨 — 읽기 요청이 LINK_DOWN 으로 끝남(해독기 안 불림)
        self.calls.append(("rr", H.REG_BLOCK_START, H.REG_BLOCK_COUNT, "low"))
        self.calls.append(("rc", H.COIL_BASE, H.COIL_COUNT, "low"))
        if self.fail_reads > 0:
            self.fail_reads -= 1
            for _b, _d, f in self.decoders:
                if f:
                    f("E402 PLC 응답 없음(일시적)")
            return
        regs = {a: self.D[a] for a in range(H.REG_BLOCK_START, H.REG_BLOCK_START + H.REG_BLOCK_COUNT)}
        coils = {a: self.M[a] for a in range(H.COIL_BASE, H.COIL_BASE + H.COIL_COUNT)}
        poll = PlcPoll(self.clock.wall(), self.clock.monotonic(), coils, regs, {})
        for _b, d, _f in self.decoders:
            d(poll)


def make_cfg(**over: Any) -> Dict[str, Any]:
    """v1 sim.make 와 같은 기준: HeaterCfg(enabled=True, log_enabled=False, **over) → v2 cfg 사전."""
    return cfg_to_dict(HeaterCfg(**{"enabled": True, "log_enabled": False, **over}))


class FastHeaterRig:
    def __init__(self, gain: float = 2250.0, writes: bool = True, csv=None, cfg: Optional[dict] = None,
                 every_second: bool = True, **over: Any) -> None:
        """every_second=True: v1 하니스처럼 1초마다 폴링 1회(poll_once). False: 엔진 주기(on_row — RUN 1 s·정지 3 s)."""
        self.every_second = every_second
        self.clk = FakeClock()
        self.plc = LadderPlc(self.clk, gain=gain)
        self.cfg = cfg if cfg is not None else make_cfg(**over)
        self.events: List[Any] = []
        self.times: List[float] = []
        self.drv = H.HeaterDriver(self.clk, self.plc, self.cfg, writes, on_event=self._ev)
        self.eng = HeaterEngine(self.clk, self.drv, self.cfg, on_event=self._ev, csv=csv)
        self.pub: List[dict] = []

    def _ev(self, e) -> None:
        self.events.append(e)
        self.times.append(self.clk.t)

    # ── 보기 ──
    def logs(self, word: str = "") -> List[tuple]:
        """[(시각, 문구)] — v1 하니스 logs 와 같은 모양."""
        return [(t, e.detail) for t, e in zip(self.times, self.events) if e.kind == "log" and word in e.detail]

    def log_texts(self, word: str = "") -> List[str]:
        return [e.detail for e in self.events if e.kind == "log" and word in e.detail]

    def cards(self, word: str = "") -> List[tuple]:
        """[(시각, 제목, 부제, 상태)] — v1 FakeChat.cards 와 같은 모양."""
        return [(t, e.data["title"], e.data["subtitle"], e.data["status"])
                for t, e in zip(self.times, self.events) if e.kind == "card" and word in e.data["title"]]

    def cards_full(self, word: str = "") -> List[tuple]:
        """[(제목, 부제, 상태, 필드)]."""
        return [(e.data["title"], e.data["subtitle"], e.data["status"], dict(e.data.get("fields") or {}))
                for e in self.events if e.kind == "card" and word in e.data["title"]]

    def listen(self) -> List[dict]:
        self.eng.add_listener(self.pub.append)
        return self.pub

    # ── 진행 ──
    def second(self, poll: bool = True) -> None:
        self.plc.scan(1.0)
        self.clk.t += 1.0
        self.plc.read()
        self.eng.on_tick_start()
        if poll:
            if self.every_second:
                self.eng.poll_once()
                self.eng._flush()
            else:
                self.eng.on_row(None)

    def run(self, secs: int, on_step: Optional[Callable[["FastHeaterRig"], None]] = None) -> None:
        for _ in range(int(secs)):
            self.second()
            if on_step is not None:
                on_step(self)

    def until(self, cond: Callable[[], bool], limit_s: int = 20000) -> int:
        n = 0
        while not cond():
            self.second()
            n += 1
            if n > limit_s:
                raise AssertionError("시간 초과")
        return n

    def poll(self, dt: float = 1.0) -> None:
        """v1 시험의 _poll(svc, clk): 읽기 → 폴링 1회(주기 무시) → 시계 +dt."""
        self.plc.read()
        self.eng.poll_once()
        self.clk.t += dt

    def call(self, fn, *a, **k):
        box = []
        fn(*a, on_done=lambda ok, why: box.append((ok, why)), **k)
        return box[0] if box else None


class HeaterRig:
    """실제 PLC 스택 위의 히터 — PlcRig(tick=False) + HeaterDriver + HeaterEngine + SnapshotBus/TickLoop."""

    def __init__(self, cfg: Optional[dict] = None, writes: bool = True, sim=None) -> None:
        from v2.common.paramdef import defaults
        from v2.common.snapshot_bus import SnapshotBus, TickLoop
        from v2.ch12.drivers import plc as P
        from v2.ch12.params import core_heater as PH
        from v2.ch12.sim.plc_rig import PlcRig
        base = dict(defaults(PH.PARAMS), CH2_HEATER_LOG_ENABLED=False, CH2_HEATER_LOG_PERIOD_S=3.0,
                    CH2_HEATER_LOG_DIR="", CH2_HEATER_RUN_LOG_PERIOD_S=10.0)
        base.update(cfg or {})
        self.p = PlcRig(writes=writes, cfg=base, sim=sim, tick=False)
        self.c, self.sim = self.p.c, self.p.sim
        self.events: List[Any] = []
        self.rows: List[dict] = []
        self.drv = H.HeaterDriver(self.c, self.p.d, self.p.cfg, writes, on_event=self.events.append)
        self.eng = HeaterEngine(self.c, self.drv, self.p.cfg, on_event=self.events.append)
        self.pre_row: List[Callable[[dict], None]] = []
        bl = P.blocks() + H.blocks()
        self.bus = SnapshotBus(self.c, bl, [self.p.d.source()])
        self.loop = TickLoop(self.c, self.bus, sinks=[self._pre, self.rows.append, self.eng.on_row],
                             tick_hooks=[self.p.d.on_tick, self.eng.on_tick_start],
                             extra_fn=lambda t: self.eng.snapshot_fields())
        self.c.start_thread(self.loop.run, "tick")

    def _pre(self, row: dict) -> None:
        for f in list(self.pre_row):
            f(row)

    def after(self, dt: float) -> None:
        self.p.after(dt)

    def wait(self, pred, limit_s: float = 600.0) -> bool:
        return self.p.wait(pred, limit_s)

    def call(self, fn, *a, **k):
        box = []
        k["on_done"] = lambda ok, why: (box.append((ok, why)), self.c.notify())
        fn(*a, **k)
        self.c.wait_until(None, lambda: bool(box))
        return box[0]

    def cards(self, word: str = "") -> List[tuple]:
        return [(e.data["title"], e.data["subtitle"], e.data["status"]) for e in list(self.events)
                if e.kind == "card" and word in e.data["title"]]

    def log_texts(self, word: str = "") -> List[str]:
        return [e.detail for e in list(self.events) if e.kind == "log" and word in e.detail]

    def done(self) -> None:
        self.loop.stop()
        self.p.done()

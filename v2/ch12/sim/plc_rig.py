"""PLC 시험대(가상 시계) — PlcSim + SimTransport + v1 정책 worker + PlcDriver(+ DC·RF 제어기)를 묶는다. 시험·sim 실행 공용.

- tick=True 면 실제 SnapshotBus·TickLoop 를 clock 스레드로 돌린다(1초 묶음 읽기 + on_tick 하트비트) — rows 에 1초 행.
- tick=False 면 read()/second() 로 시험이 직접 읽기를 보낸다.
설정 사전은 params plc·dc·rf 기본값 + CH2 기능 플래그(v1 운영값: CH2_SUPPORTS_HEATER·DC2 true).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.common.snapshot_bus import SnapshotBus, TickLoop
from v2.common.transport import SimTransport
from v2.common.worker import Request
from v2.ch12.drivers import plc as P
from v2.ch12.params import dc as PDC, plc as PPLC, rf as PRF
from v2.ch12.sim.plc_sim import PlcSim

BASE_CFG: Dict[str, Any] = dict(defaults(PPLC.PARAMS + PDC.PARAMS + PRF.PARAMS),
                                CH2_SUPPORTS_HEATER=True, CH2_SUPPORTS_DC2=True)


class CountingSimTransport(SimTransport):
    def __init__(self, clock, device) -> None:
        super().__init__(clock, device)
        self.opens = 0
        self.closes = 0

    def open(self) -> None:
        super().open()
        self.opens += 1

    def close(self) -> None:
        if self._open:
            self.closes += 1
        super().close()


class PlcRig:
    def __init__(self, writes: bool = True, cfg: Optional[dict] = None, sim: Optional[PlcSim] = None,
                 tick: bool = False, dc: bool = False, rf: bool = False, start_s: float = 0.5) -> None:
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.cfg = dict(BASE_CFG, **(cfg or {}))
        self.sim = sim or PlcSim()
        self.events: List[Any] = []
        self.wevents: List[Any] = []
        self.inner = CountingSimTransport(self.c, self.sim)
        self.tr = P.make_transport(self.c, self.cfg, self.inner)
        self.w = P.make_worker(self.tr, self.cfg, writes, on_event=self.wevents.append)
        self.d = P.PlcDriver(self.c, self.w, self.cfg, writes, on_event=self.events.append)
        self.dc1 = self.dc2 = self.rf = None
        self.pevents: List[Any] = []
        if dc:
            from v2.ch12.drivers.dc import DcPower
            self.dc1 = DcPower(self.c, self.d, self.cfg, writes, self.pevents.append, unit=1)
            self.dc2 = DcPower(self.c, self.d, self.cfg, writes, self.pevents.append, unit=2)
        if rf:
            from v2.ch12.drivers.rf import RfPower
            self.rf = RfPower(self.c, self.d, self.cfg, writes, self.pevents.append, mode="ch2")
        self.rows: List[dict] = []
        self.loop = None
        self.w.start()
        if tick:
            from v2.ch12.drivers import dc as DCM, rf as RFM
            bl = P.blocks() + (DCM.blocks() if dc else []) + (RFM.blocks() if rf else [])
            self.bus = SnapshotBus(self.c, bl, [self.d.source()])
            self.loop = TickLoop(self.c, self.bus, sinks=[self.rows.append], tick_hooks=[self.d.on_tick])
            self.c.start_thread(self.loop.run, "tick")
        self.c.wait_until(start_s)

    # ───────── 도우미 ─────────
    def kinds(self) -> List[str]:
        return [e.kind for e in self.events]

    def wait(self, pred, limit_s: float = 600.0) -> bool:
        return self.c.wait_until(self.c.monotonic() + limit_s, pred)

    def until(self, t: float) -> None:
        self.c.wait_until(t)
        self.sim.advance_to(self.c.monotonic())

    def after(self, dt: float) -> None:
        self.until(self.c.monotonic() + dt)

    def call(self, fn, *a, **k):
        """요청을 내는 드라이버 메서드를 부르고 결과(Result)를 기다린다."""
        box = []
        k["on_done"] = lambda r: (box.append(r), self.c.notify())
        fn(*a, **k)
        self.c.wait_until(None, lambda: bool(box))
        return box[0]

    def read(self, deadline_s: float = 0.9, split: bool = True):
        """1초 읽기 한 번 — 실행 프로그램과 같은 나눠 읽기(split, source_start) 또는 요청 하나(source_read)."""
        box = []
        cb = lambda r: (box.append(r), self.c.notify())
        if split:
            self.d.source_start(0, self.c.monotonic() + deadline_s, cb)
        else:
            self.w.submit(Request("read", lambda s: self.d.source_read(s, 0), deadline=self.c.monotonic() + deadline_s,
                                  on_done=cb, name="read:plc"))
        self.c.wait_until(None, lambda: bool(box))
        return box[0]

    def second(self, n: int = 1) -> None:
        """1초씩: 읽기 → tick 훅(하트비트) → 다음 정각까지."""
        for _ in range(n):
            self.read()
            self.d.on_tick(None)
            self.c.wait_until(self.c.monotonic() + 1.0 - (self.c.monotonic() % 1.0))

    def pdus(self, since: float = -1.0) -> List[bytes]:
        return [f[7:] for t, f in self.sim.frames if t >= since]

    def frames(self, since: float = -1.0):
        return [(t, f[7:]) for t, f in self.sim.frames if t >= since]

    def done(self) -> None:
        if self.loop is not None:
            self.loop.stop()
        self.w.stop()

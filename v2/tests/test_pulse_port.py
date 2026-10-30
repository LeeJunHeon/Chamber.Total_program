"""펄스 포트 소유(pulse_port) 시험 — CH1·CH2 의 DC Pulse·RF Pulse 가 MOXA 포트를 나눠 쓰는 규칙(v1 runtime_state 점유).

- 같은 주소를 같은 챔버의 다른 종류·다른 챔버가 잡으면 거부(v1 문구), 같은 챔버·같은 종류는 다시 잡기 허용.
- 잡을 때만 연결(그 종류의 전송·Worker 를 새로), 풀면 Worker 를 멈추고 1초 행은 off 품질.
- CH1(4007)·CH2(4005)는 주소가 달라 동시에 돈다.
"""
import socket
import threading

from v2.ch12.drivers import dcpulse as D
from v2.ch12.drivers import rfpulse as R
from v2.ch12.drivers.pulse_port import PulsePorts, endpoint, tcp_transport_factory
from v2.ch12.params import dcpulse as PD
from v2.ch12.params import rfpulse as PR
from v2.ch12.sim.chamber import Chamber
from v2.ch12.sim.dcpulse_sim import DcPulseSim
from v2.ch12.sim.rfpulse_sim import RfPulseSim
from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop
from v2.common.transport import SimTransport

CFG = defaults(PD.PARAMS + PR.PARAMS)


class Plant:
    def __init__(self, cfg=None):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.cfg = dict(CFG, **(cfg or {}))
        self.ch = {n: Chamber(n) for n in ("ch1", "ch2")}
        for ch in self.ch.values():
            ch.gas_sccm["ar"] = 20.0
            ch.pressure_override = 0.005
        self.sims = {}
        self.made = []
        for n in ("ch1", "ch2"):
            self.sims[(n, "dcpulse")] = DcPulseSim(self.ch[n])
            self.sims[(n, "rfpulse")] = RfPulseSim(self.ch[n], control=2)
        self.events = []
        self.ports = PulsePorts(self.c, self._factory, True, on_event=self.events.append)
        self.drv, self.src, blocks = {}, {}, []
        for n in ("ch1", "ch2"):
            for kind, mod, cls in (("dcpulse", D, D.DcPulseDriver), ("rfpulse", R, R.RfPulseDriver)):
                d = cls(self.c, None, self.cfg, True, on_event=self.events.append, chamber=n)
                s = Source(f"{n}.{kind}", None, d.source_read, (f"{n}.{kind}",))
                self.drv[(n, kind)], self.src[(n, kind)] = d, s
                blocks += mod.blocks(n)
                self.ports.add(kind, n, d, s)
        self.bus = SnapshotBus(self.c, blocks, list(self.src.values()))

    def _factory(self, kind, chamber):
        self.made.append((chamber, kind))
        return SimTransport(self.c, self.sims[(chamber, kind)])

    def rows(self, n):
        out = []
        TickLoop(self.c, self.bus, [out.append]).run(n)
        return out

    def call(self, fn, *a, **kw):
        box = []
        fn(*a, on_done=lambda *r: (box.append(r), self.c.notify()), **kw)
        self.c.wait_until(None, lambda: bool(box))
        return box[0]

    def done(self):
        for p in self.ports.ports.values():
            p.release()


def test_ports_by_endpoint_v1_addresses():
    p = Plant()
    assert sorted(p.ports.ports) == ["192.168.1.50:4005", "192.168.1.50:4007"]
    assert endpoint(p.cfg, "ch1", "dcpulse") == ("192.168.1.50", 4007) == endpoint(p.cfg, "ch1", "rfpulse")
    assert endpoint(p.cfg, "ch2", "dcpulse") == ("192.168.1.50", 4005) == endpoint(p.cfg, "ch2", "rfpulse")
    p.done()


def test_unclaimed_is_off_and_not_connected():
    p = Plant()
    row = p.rows(2)[-1]
    assert all(row[f"{n}.{k}.q"] == "off" for n in ("ch1", "ch2") for k in ("dcpulse", "rfpulse"))
    assert p.made == [] and all(len(s.received) == 0 for s in p.sims.values())
    p.done()


def test_same_port_other_kind_rejected_with_v1_message_and_reclaim_ok():
    p = Plant()
    assert p.ports.claim("dcpulse", "ch1") == (True, "", None)
    w = p.ports.port_of("dcpulse", "ch1").worker
    ok, msg, owner = p.ports.claim("rfpulse", "ch1")
    assert not ok and owner == {"ch": "ch1", "kind": "dcpulse"}
    assert msg == "RF-Pulse 시작 불가: 동일 주소(192.168.1.50:4007) 장비를 CH1(DC-Pulse) 공정이 사용 중입니다."
    assert p.ports.claim("dcpulse", "ch1")[0] and p.ports.port_of("dcpulse", "ch1").worker is w     # 다시 잡기
    row = p.rows(2)[-1]
    assert row["ch1.dcpulse.q"] == "ok" and row["ch1.rfpulse.q"] == "off"
    assert p.made == [("ch1", "dcpulse")]
    p.done()


def test_other_chamber_same_endpoint_rejected():
    p = Plant(cfg={"CH2_DCPULSE_TCP_PORT": 4007})                    # 설정을 바꿔 CH2 DC Pulse 도 4007 이면
    assert p.ports.port_of("dcpulse", "ch2") is p.ports.port_of("dcpulse", "ch1")
    assert p.ports.claim("dcpulse", "ch1")[0]
    ok, msg, owner = p.ports.claim("dcpulse", "ch2")
    assert not ok and msg == "DC-Pulse 시작 불가: 동일 주소(192.168.1.50:4007) 장비를 CH1(DC-Pulse) 공정이 사용 중입니다."
    p.done()


def test_release_then_other_kind_on_same_port():
    p = Plant()
    assert p.ports.claim("dcpulse", "ch1")[0]
    d = p.drv[("ch1", "dcpulse")]
    assert p.call(d.prepare_and_start, 100.0, freq=50, off_time_us=5.0) == (None, True)
    assert p.call(d.output_off) == (None, True)
    assert p.ports.release_chamber("ch1") == 1
    assert d.worker is None and p.src[("ch1", "dcpulse")].worker is None
    assert p.rows(1)[-1]["ch1.dcpulse.q"] == "off"
    assert p.ports.claim("rfpulse", "ch1")[0]                       # 케이블을 RF Pulse 로 — 같은 포트에 새 Worker
    rf = p.drv[("ch1", "rfpulse")]
    assert p.call(rf.start_pulse_process, 100.0) == (None, True)
    row = p.rows(3)[-1]
    assert row["ch1.rfpulse.q"] == "ok" and row["ch1.dcpulse.q"] == "off"
    assert p.made == [("ch1", "dcpulse"), ("ch1", "rfpulse")]
    p.done()


def test_ch1_and_ch2_run_concurrently():
    p = Plant()
    assert p.ports.claim("dcpulse", "ch1")[0] and p.ports.claim("rfpulse", "ch2")[0]
    d1, r2 = p.drv[("ch1", "dcpulse")], p.drv[("ch2", "rfpulse")]
    box = []
    d1.prepare_and_start(150.0, freq=50, off_time_us=5.0, on_done=lambda *x: (box.append(("d", x)), p.c.notify()))
    r2.start_pulse_process(80.0, on_done=lambda *x: (box.append(("r", x)), p.c.notify()))
    p.c.wait_until(None, lambda: len(box) == 2)
    assert sorted(b[0] for b in box) == ["d", "r"] and all(b[1] == (None, True) for b in box)
    row = p.rows(12)[-1]
    assert row["ch1.dcpulse.p_w"] == 150.0 and row["ch2.rfpulse.fwd_w"] is not None
    assert p.sims[("ch1", "dcpulse")].out and p.sims[("ch2", "rfpulse")].out
    assert len(p.sims[("ch1", "rfpulse")].received) == 0 and len(p.sims[("ch2", "dcpulse")].received) == 0
    p.done()


def test_release_from_worker_callback_thread():
    p = Plant()
    assert p.ports.claim("rfpulse", "ch2")[0]
    rf = p.drv[("ch2", "rfpulse")]
    done = []

    def cb(reason, ok):
        done.append(p.ports.release_chamber("ch2"))                  # worker 스레드 안에서 해제(스스로 join 하지 않음)
        p.c.notify()
    rf.stop_unused(on_done=cb)
    p.c.wait_until(None, lambda: bool(done))
    assert done == [1] and rf.worker is None
    assert p.ports.claim("dcpulse", "ch2")[0]
    p.done()


def test_tcp_factory_keepalive_rules():
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(4)
    port = srv.getsockname()[1]
    acc = []
    th = threading.Thread(target=lambda: [acc.append(srv.accept()[0]) for _ in range(2)], daemon=True)
    th.start()
    c = VirtualClock()
    cfg = dict(CFG, DCPULSE_TCP_HOST="127.0.0.1", RFPULSE_TCP_HOST="127.0.0.1", DCPULSE_TCP_PORT=port, CH1_RFPULSE_TCP_PORT=port)
    make = tcp_transport_factory(c, cfg)
    t_d, t_r = make("dcpulse", "ch1"), make("rfpulse", "ch1")
    assert t_d.settings.connect_timeout_s == 3.0 and t_r.settings.connect_timeout_s == 1.5
    t_d.open()
    t_r.open()
    try:
        assert t_d._sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 0      # DCP_TCP_KEEPALIVE false
        assert t_r._sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0      # RF Pulse 늘 1
    finally:
        t_d.close()
        t_r.close()
        th.join(2.0)
        for s in acc:
            s.close()
        srv.close()

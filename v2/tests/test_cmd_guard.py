"""챔버12 통신층 명령 가드 — bench 질의 목록·드라이버 명령 목록(prod·sim)·금지 명령(가상 시계·SimTransport — 실제 장비 접속 없음).

1) 판정 단위 시험(장비별 허용·금지, 사유).
2) 모든 골든 송신 바이트(v1 을 실제로 돌려 받은 tests/golden/dev_*.json)가 prod 목록을 통과한다.
3) bench: 장비마다 쓰기 경로를 불러도 포트에 나간 바이트 중 bench 목록 밖은 0 바이트(worker·드라이버 거부 + 가드).
4) sim(prod 목록): 금지·목록 밖 명령을 드라이버 경로(worker 요청)로 보내도 포트에 닿기 전에 막히고(COMMAND_BLOCKED) 연결은 유지.
"""
import ast
import json
from pathlib import Path

import pytest

from v2.common import aebus
from v2.common import modbus_tcp as MT
from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.common.transport import SimTransport
from v2.common.worker import Request, Worker
from v2.ch12 import cmd_guard as G
from v2.ch12.drivers import dcpulse as DCP, dcpulse_proto as DP, ig as IGD, mfc as MFCD, plc as PLCD, rfpulse as RFP
from v2.ch12.drivers import plc_map as PM, stm100 as STM, stm100_proto as SP, tsp as TSPD
from v2.ch12.params import PARAMS
from v2.ch12.sim.chamber import Chamber
from v2.ch12.sim.dcpulse_sim import DcPulseSim
from v2.ch12.sim.ig_sim import IgSim
from v2.ch12.sim.mfc_sim import MfcSim
from v2.ch12.sim.plc_sim import PlcSim
from v2.ch12.sim.rfpulse_sim import RfPulseSim
from v2.ch12.sim.stm100_sim import Stm100Sim
from v2.ch12.sim.tsp_sim import TspSim

CFG = defaults(PARAMS)
GOLD = Path(__file__).parent / "golden"


def _ev(x):
    return x if not isinstance(x, str) else ast.literal_eval(x)


def mb(pdu: bytes, unit: int = 1) -> bytes:
    return MT.mbap(7, unit, pdu)


def plc(b, mode="prod", cfg=CFG):
    return G.checker("plc", mode, cfg)(b)


def line(dev, s, mode="prod"):
    return G.checker(dev, mode, CFG)((s + "\r").encode())


# ───────── 1) 판정 단위 ─────────
def test_plc_lists():
    rd = mb(MT.pdu_read_regs(0, 12))
    assert plc(rd, "bench") is None and plc(mb(MT.pdu_read_coils(1, 481)), "bench") is None
    assert plc(mb(MT.pdu_read_regs(0, 12), unit=2), "bench")                               # 유닛
    assert "개수" in plc(mb(bytes([1, 0, 0, 0x07, 0xD1])), "bench")                               # 2001 개
    wr = mb(MT.pdu_write_coil(PM.COIL_MAP["Ar_1_GAS_SW"], True))
    assert "FC1·FC3" in plc(wr, "bench") and plc(wr) is None
    assert "P 미러" in plc(mb(MT.pdu_write_coil(803, True)))
    assert plc(mb(MT.pdu_write_reg(6, 17))) is None                                          # DCV_WRITE_2
    assert "DCV_READ" in plc(mb(MT.pdu_write_reg(0, 1)))                                      # A/D 입력
    assert plc(mb(MT.pdu_write_reg(112, 500))) is None and plc(mb(MT.pdu_write_reg(160, 0x1234))) is None   # 히터 SV·세션 마커
    assert plc(mb(MT.pdu_write_coil(640, True))) is None and "히터" in plc(mb(MT.pdu_write_coil(642, True)))
    assert "히터" in plc(mb(MT.pdu_write_reg(110, 1)))                                        # PV(읽기 전용)
    off = dict(CFG, CH2_SUPPORTS_HEATER=False)
    assert plc(mb(MT.pdu_write_reg(112, 500)), cfg=off) and plc(mb(MT.pdu_write_coil(640, True)), cfg=off)
    assert "목록 밖" in plc(mb(MT.pdu_write_reg(200, 1)))
    assert "FC15" in plc(mb(MT.pdu_write_coils(203, [True, False])))
    assert "길이" in plc(rd[:-1] + b"\x00\x00")


def test_mfc_ig_lists():
    assert len(G.MFC_BENCH) == 15 and all(line("mfc", q, "bench") is None for q in G.MFC_BENCH)
    for no in ("Q1 10.0", "L11", "O", "Z1", "R64", "R99"):
        assert line("mfc", no, "bench"), no
    for ok in ("Q1 10.0", "Q2 0.7000000000000001", "Q3 1e-05", "L11", "L30", "L51", "L71", "Z1", "O", "C", "D4", "S4 0.123", "R60", "R7"):
        assert line("mfc", ok) is None, ok
    for bad in ("L00000", "E03", "F0", "G2", "W51", "Z2", "Q5 10.0", "Q4 1.0", "D6", "V50"):
        why = line("mfc", bad)
        assert why and "금지" in why, (bad, why)
    assert "목록 밖" in line("mfc", "H") and "목록 밖" in line("mfc", "L41")
    assert G.checker("mfc", "prod", CFG)(b"R60\n")                                          # CR 아님
    assert line("ig", "RDI", "bench") is None and line("ig", "SIG 1", "bench") and line("ig", "SIG 0", "bench")
    assert line("ig", "SIG 1") is None and line("ig", "SIG 0") is None and line("ig", "SIG 2")


def test_pulse_tsp_stm_lists():
    d = G.checker("dcpulse", "prod", CFG)
    db = G.checker("dcpulse", "bench", CFG)
    assert all(db(DP.pack_read(c)) is None for c in G.DCP_BENCH) and len(G.DCP_BENCH) == 15
    assert "bench" in db(DP.pack_write(0x80, 1, 2)) and d(DP.pack_write(0x80, 1, 2)) is None
    assert "금지" in d(DP.pack_write(0x7B, 1, 2)) and d(DP.pack_write(0x7B, 3, 2)) is None
    assert "금지" in d(DP.pack_write(0x0C, 50, 2)) and "금지" in d(DP.pack_write(0x81, 1, 2))   # 한계 설정·Regulation V
    assert "값 밖" in d(DP.pack_write(0x83, 101, 2)) and "목록 밖" in d(DP.pack_write(0x70, 1, 2))
    assert "CHK" in d(DP.pack_read(0x90)[:-1] + b"\x00")
    r = G.checker("rfpulse", "prod", CFG)
    rb = G.checker("rfpulse", "bench", CFG)
    assert all(rb(aebus.build(1, c)) is None for c in G.RFP_BENCH)
    assert rb(aebus.build(1, 1)) and r(aebus.build(1, 1)) is None and r(aebus.build(1, 3, b"\x06")) is None
    assert "금지" in r(aebus.build(1, 3, b"\x07")) and "금지" in r(aebus.build(1, 33, b"\x0a"))
    assert r(aebus.build(1, 200)) and "주소" in r(aebus.build(2, 162)) and "데이터 밖" in r(aebus.build(1, 27, b"\x06"))
    t = G.checker("tsp", "prod", CFG)
    tb = G.checker("tsp", "bench", CFG)
    st = TSPD.build_frame("205", False, None)
    assert tb(st) is None and tb(TSPD.build_frame("011", True, "1")) and t(TSPD.build_frame("011", True, "1")) is None
    assert "금지" in t(TSPD.build_frame("012", True, "1")) and tb(TSPD.build_frame("206", False, None))
    assert "CRC" in t(st[:-1] + b"0") and "주소" in t(TSPD.build_frame("205", False, None, addr=0x81))
    s = G.checker("stm100", "prod", CFG)
    sb = G.checker("stm100", "bench", CFG)
    assert all(sb(SP.build_frame(q)) is None for q in G.STM_BENCH)
    assert "bench 금지" in sb(SP.build_frame("L")) and s(SP.build_frame("L")) is None
    assert sb(SP.build_frame("E=8.9")) and s(SP.build_frame("E=8.9")) is None and s(SP.build_frame("B")) is None
    assert "목록 밖" in s(SP.build_frame("X")) and "체크섬" in s(SP.build_frame("S")[:-1] + b"\x00")


def test_driver_command_sets_match_guard():
    """드라이버가 읽는 명령 ⊂ bench 목록, 드라이버 쓰기 명령 = prod 쓰기 목록."""
    reads = {v for k, v in MFCD.MFC_COMMANDS.items() if k.startswith("READ_") and isinstance(v, str)}
    reads |= {MFCD.mk_cmd(k, ch) for k in ("READ_FLOW", "READ_FLOW_SET") for ch in (1, 2, 3)}
    assert reads == set(G.MFC_BENCH) and G.MFC_PROD_QUERIES <= set(G.MFC_BENCH)
    assert set(DP.READ_LABELS) == G.DCP_BENCH
    assert {c for c in RFP.CMD_NAMES if c >= 128} <= G.RFP_BENCH
    assert {c for c in RFP.CMD_NAMES if c < 128} == set(G.RFP_PROD_WRITES)
    assert TSPD.WIN_STATUS in G.TSP_BENCH_WINS and {w for w, _ in G.TSP_PROD_WRITES} == {TSPD.WIN_ONOFF}
    assert set(G.IG_PROD) == {"RDI", "SIG 1", "SIG 0"} and set(G.allowed_counts()) == set(G.DEVICES)
    from v2.ch12.drivers import heater as HTR
    assert G.HEATER_WRITE_REGS == HTR.WRITE_REGS and G.HEATER_WRITE_COILS == HTR.WRITE_COILS
    coils, regs = G.plc_writable(CFG)
    assert coils >= set(PM.COIL_MAP.values()) and not any(PM.is_p_mirror(a) for a in coils)
    assert regs == G.PLC_DCV_WRITE_REGS | G.HEATER_WRITE_REGS | {CFG["CH2_HEATER_SESSION_MARK_REG"]}


# ───────── 2) 골든 송신 바이트 = prod 목록 안 ─────────
def test_all_golden_sent_bytes_pass_prod():
    load = lambda n: json.loads((GOLD / n).read_text(encoding="utf-8"))
    sent = []
    d = load("dev_plc.json")
    for c in d["plc"]["cases"]:
        sent += [("plc", mb(bytes.fromhex(p)), CFG) for p in c["pdus"]]
    for sec in ("dc", "rf"):
        for c in d[sec]:
            sent += [("plc", mb(bytes.fromhex(p)), CFG) for p in c["writes"]]
    for name, dev in (("dev_mfc.json", "mfc"), ("dev_ig.json", "ig")):
        for c in load(name)["cases"]:
            sent += [(dev, bytes.fromhex(h), CFG) for h in c["sent"]]
    for name, dev in (("dev_dcpulse.json", "dcpulse"), ("dev_rfpulse.json", "rfpulse")):
        for c in load(name)["cases"]:
            sent += [(dev, bytes.fromhex(h), CFG) for h in _ev(c["sent"])]
    for c in load("dev_tsp.json")["cases"]:
        cc = dict(CFG, **_ev(c["cfg"]))
        sent += [("tsp", bytes.fromhex(t["hex"]), cc) for t in _ev(c["tx"])]
    for c in load("dev_stm100.json")["device"]:
        sent += [("stm100", bytes.fromhex(t["hex"]), CFG) for t in c["tx"]]
    bad = [(dev, b.hex(" "), G.checker(dev, "prod", cc)(b)) for dev, b, cc in sent if G.checker(dev, "prod", cc)(b)]
    assert bad == []
    assert {dev for dev, _b, _c in sent} == set(G.DEVICES) and len(sent) > 400
    reads = [(dev, b) for dev, b, cc in sent if G.checker(dev, "bench", cc)(b) is None]
    assert reads and all(dev != "plc" or b[7] in (1, 3) for dev, b in reads)     # bench 를 통과한 것은 읽기뿐


# ───────── 3)·4) 드라이버 경로 ─────────
class Dev:
    """장비 하나 — 드라이버 + worker + 가드(mode) + SimTransport. port 에 실제로 닿은 바이트를 모은다."""

    def __init__(self, kind, mode, writes):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.kind, self.events, self.port = kind, [], []
        cfg = dict(CFG)
        ch = Chamber("ch2")
        sims = {"plc": lambda: PlcSim(), "mfc": lambda: MfcSim(ch), "ig": lambda: IgSim(ch), "dcpulse": lambda: DcPulseSim(ch),
                "rfpulse": lambda: RfPulseSim(ch), "tsp": lambda: TspSim(), "stm100": lambda: Stm100Sim(ch)}
        self.sim = sims[kind]()
        inner = self.sim.make_transport(self.c) if kind == "tsp" else SimTransport(self.c, self.sim)
        orig = inner.write
        inner.write = lambda data: (self.port.append(bytes(data)), orig(data))
        name = {"mfc": "ch2.mfc", "ig": "ch2.ig", "dcpulse": "ch2.dcpulse", "rfpulse": "ch2.rfpulse", "stm100": "ch2.stm100"}.get(kind, kind)
        self.g = G.GuardedTransport(inner, name, mode, cfg, on_event=self.events.append)
        ev = self.events.append
        if kind == "plc":
            self.w = PLCD.make_worker(PLCD.make_transport(self.c, cfg, inner=self.g), cfg, writes, on_event=ev)
            self.d = PLCD.PlcDriver(self.c, self.w, cfg, writes, on_event=ev)
        elif kind == "mfc":
            self.w = MFCD.make_worker(self.c, self.g, cfg, "ch2", writes, on_event=ev)
            self.d = MFCD.MfcDriver(self.c, self.w, cfg, writes, on_event=ev, chamber="ch2")
        elif kind == "ig":
            self.w = IGD.make_worker(self.c, self.g, cfg, "ch2", writes, on_event=ev)
            self.d = IGD.IgDriver(self.c, self.w, cfg, writes, on_event=ev, chamber="ch2")
        elif kind == "tsp":
            self.w = Worker("tsp", TSPD.OnDemandTransport(self.c, lambda _k: self.g), writes_allowed=writes, on_event=ev)
            self.d = TSPD.TspDriver(self.c, self.w, cfg, writes, on_event=ev)
        elif kind == "stm100":
            self.w = Worker("ch2.stm100", self.g, writes_allowed=writes, on_event=ev, **STM.worker_kwargs())
            self.d = STM.Stm100Driver(self.c, self.w, cfg, writes, on_event=ev, chamber="ch2")
        else:
            cls = DCP.DcPulseDriver if kind == "dcpulse" else RFP.RfPulseDriver
            self.d = cls(self.c, None, cfg, writes, on_event=ev, chamber="ch2")
            self.w = Worker(name, self.g, writes_allowed=writes, on_event=ev, **self.d.worker_params())
            self.d.bind(self.w)
        self.w.start()
        self.c.wait_until(1.5)
        self.results = []

    def cb(self, *r):
        self.results.append(r)
        self.c.notify()

    def raw(self, fn, kind="write"):
        box = []
        self.w.submit(Request(kind, fn, deadline=self.c.monotonic() + 5.0, name="raw",
                              on_done=lambda r: (box.append(r), self.c.notify())))
        self.c.wait_until(self.c.monotonic() + 6.0, lambda: bool(box))
        return box[0]

    def done(self):
        self.w.stop()
        self.c.unregister()


def _bench_writes(dv):
    d, cb = dv.d, dv.cb
    k = dv.kind
    if k == "plc":
        for f in (lambda: d.write_coil(203, True, on_done=cb), lambda: d.write_reg(6, 10, on_done=cb),
                  lambda: d.gas(1, "AR", on=True, on_done=cb), lambda: d.write_coil(640, True, on_done=cb)):
            f()
    elif k == "mfc":
        d.set_flow(1, 10.0, on_done=cb), d.flow_on(1, on_done=cb), d.valve_open(on_done=cb), d.sp_set(1, 5.0, on_done=cb)
        d.ps_zeroing(on_done=cb), d.mfc_zeroing(1, on_done=cb)
    elif k == "ig":
        d.ensure_on(on_done=cb), d.ensure_off(on_done=cb)
    elif k == "tsp":
        d.on(on_done=cb), d.off(on_done=cb), d.read_status(on_done=cb)
    elif k == "stm100":
        d.zero("B", on_done=cb), d.set_material(8.9, 1.0, 100.0, on_done=cb)
    elif k == "dcpulse":
        d.prepare_and_start(100.0, freq=50, off_time_us=5.0, on_done=cb), d.output_off(on_done=cb), d.set_limits(100, 1, 600, on_done=cb)
    else:
        d.start_pulse_process(100.0, 1000, 50, on_done=cb), d.stop_process(on_done=cb)


READS = {     # 드라이버의 읽기 한 번(worker 스레드 안 fn)
    "plc": lambda dv: (lambda s: dv.d.source_read(s, 0)),
    "mfc": lambda dv: (lambda s: dv.d.io_query(s, MFCD.Query("R5", 2.0, 0.0, ("P",), 0, "[T]"))),
    "ig": lambda dv: (lambda s: dv.d.io_query(s, IGD.Query("RDI", 3.0, 0.0, (), 0, "[T]"))),
    "dcpulse": lambda dv: (lambda s: dv.d._q(s, DP.R_STATUS)),
    "rfpulse": lambda dv: (lambda s: dv.d._transact(s, RFP.Q_STATUS, b"", 3.0, dv.c.monotonic() + 4.0, "[T]", False)),
    "tsp": lambda dv: (lambda s: dv.d._get_status(s)),
    "stm100": lambda dv: (lambda s: dv.d._query(s, "S")),
}


@pytest.mark.parametrize("kind", G.DEVICES)
def test_bench_write_paths_send_zero_bytes_outside_bench_list(kind):
    dv = Dev(kind, "bench", writes=False)
    try:
        _bench_writes(dv)
        dv.c.wait_until(dv.c.monotonic() + 5.0)
        # 드라이버가 실수로 쓰기 바이트를 읽기 요청에 실어도 가드가 막는다(마지막 관문)
        bad = {"plc": mb(MT.pdu_write_coil(203, True)), "mfc": b"Q1 10.0\r", "ig": b"SIG 1\r",
               "dcpulse": DP.pack_write(0x80, 1, 2), "rfpulse": aebus.build(1, 2), "tsp": TSPD.build_frame("011", True, "1"),
               "stm100": SP.build_frame("B")}[kind]
        r = dv.raw(lambda s: s.write(bad), kind="read")
        assert r.reason == "COMMAND_BLOCKED"
        rd = dv.raw(READS[kind](dv), kind="read")                              # 읽기는 나간다(헛시험 아님)
        assert rd.reason is None, rd
        chk = G.checker(dv.g.device, "bench", CFG)
        assert dv.port and [b for b in dv.port if chk(b)] == []
        assert bad not in dv.port and dv.results
        refused = [x for x in dv.results if "BENCH_READ_ONLY" in str(x)]
        assert len(refused) == len(dv.results) - (1 if kind == "tsp" else 0), dv.results     # TSP read_status 만 통과
        assert dv.w.writes_allowed is False and dv.d.writes_allowed is False
    finally:
        dv.done()


FORBIDDEN = {
    "plc": [mb(MT.pdu_write_coil(803, True)), mb(MT.pdu_write_reg(0, 5)), mb(MT.pdu_write_regs(4, [1, 2]))],
    "mfc": [b"E03\r", b"L00000\r", b"Q5 10.0\r"],
    "ig": [b"SIG 2\r", b"X\r"],
    "dcpulse": [DP.pack_write(0x05, 10, 2), DP.pack_write(0x7B, 1, 2)],
    "rfpulse": [aebus.build(1, 33, b"\x0a"), aebus.build(1, 3, b"\x07")],
    "tsp": [TSPD.build_frame("012", True, "1")],
    "stm100": [SP.build_frame("X")],
}


@pytest.mark.parametrize("kind", G.DEVICES)
def test_forbidden_commands_blocked_before_port_link_kept(kind):
    dv = Dev(kind, "sim", writes=True)
    try:
        for bad in FORBIDDEN[kind]:
            r = dv.raw(lambda s, bad=bad: s.write(bad))
            assert r.reason == "COMMAND_BLOCKED", (kind, bad, r)
            assert bad not in dv.port
        if kind == "mfc":
            r = dv.raw(lambda s: dv.d._write(s, "E03"))                    # 드라이버 쓰기 함수로도
            assert r.reason == "COMMAND_BLOCKED"
        if kind == "dcpulse":
            dv.d.set_limits(100, 1, 600, on_done=dv.cb)                    # v1 이 부르지 않는 설정 API
            dv.c.wait_until(dv.c.monotonic() + 5.0, lambda: bool(dv.results))
            assert 0x0C not in dv.sim.cmds_seen()
        blocked = [e for e in dv.events if isinstance(e, G.GuardEvent)]
        assert len(blocked) >= len(FORBIDDEN[kind]) and all(e.reason == "COMMAND_BLOCKED" for e in blocked)
        assert dv.w.connected                                              # 연결은 유지
    finally:
        dv.done()

"""bench 실측 도구의 장비 조립·장비별 질의(v2/tools/bench_probe.py 가 쓴다) — 읽기 전용.

장비 하나(Probe) = 주소 → TcpTransport → 통신층 가드(v2.ch12.cmd_guard, mode bench) → 녹화(RecordingTransport) → worker(writes_allowed
False) → 드라이버(writes_allowed False). 가드가 포트 바로 위라 막힌 바이트는 포트에도 녹화에도 닿지 않는다(이벤트로 남음).
PLC 는 녹화 바깥에 v1 재접속 정책 전송(RetryOpenTransport), TSP 는 필요할 때만 여는 전송(OnDemandTransport)이 감싼다.
장비별 질의는 드라이버의 한 번 주고받기 함수(io_query·_q·_transact·_get_status·_query)만 쓴다 — 쓰는 함수는 부르지 않는다.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from v2.common import aebus
from v2.common.transport import RecordingTransport, ReadTimeout, TcpSettings, TcpTransport
from v2.common.worker import Worker
from v2.ch12 import cmd_guard as G
from v2.ch12.drivers import dcpulse as DCP, ig as IGD, mfc as MFCD, plc as PLCD, pulse_port as PP, rfpulse as RFP
from v2.ch12.drivers import stm100 as STM, tsp as TSPD
from v2.ch12.drivers import dcpulse_proto as DP
from v2.ch12.drivers.line_ops import Query
from v2.ch12.drivers.rfpulse_poll import parse_status

# 장비 순서 — 펄스(ch1.pulse·ch2.pulse)는 --ch1-pulse/--ch2-pulse 로 종류를 고른 때만(4007·4005 에 어느 쪽이 꽂혔는지
# 코드로 알 수 없어 다른 프로토콜 프레임을 보내지 않으려고 기본 제외). TSP 운전 IG 는 CH1 IG 와 같은 포트라 따로 열지 않는다.
PROBES = ("plc", "ch1.mfc", "ch2.mfc", "ch1.ig", "ch2.ig", "tsp", "ch2.stm100", "ch1.pulse", "ch2.pulse")
TEXT_KINDS = ("mfc", "ig")                         # 녹화를 글자로 보여 줄 장비(나머지는 hex)
RFP_QUERY_NAMES = {128: "기종(ASCII)", 130: "용량 W", 152: "반사 자동 차단 시간(명령 33 값)", 164: "REPORT_SETPOINT",
                   167: "REPORT_DELIVERED", 177: "REPORT_PULSING", 198: "SW 판"}   # 챔버K CESAR 표·v1 device/rf_pulse.py:113-125


@dataclass
class Probe:
    name: str                      # 'plc'·'ch1.mfc'·'ch1.rfpulse' …
    kind: str                      # 가드 장비 이름
    chamber: str
    host: str
    port: int
    rec: Optional[RecordingTransport] = None
    guard: Optional[G.GuardedTransport] = None
    outer: Any = None              # worker 가 쓰는 전송
    worker: Optional[Worker] = None
    driver: Any = None
    blocks: tuple = ()
    source: Any = None             # snapshot_bus.Source 또는 (read 함수, 마감 ms)
    state: Dict[str, Any] = field(default_factory=dict)

    @property
    def endpoint(self) -> str:
        return f"{self.host}:{self.port}"


def endpoints(cfg: Dict[str, Any], pulse: Dict[str, str]) -> Dict[str, Tuple[str, str, str, int]]:
    """이름 → (가드 장비, 챔버, host, port). pulse = {'ch1': 'rfpulse'|'dcpulse'|'none', 'ch2': …}."""
    out: Dict[str, Tuple[str, str, str, int]] = {"plc": ("plc", "shared", str(cfg["PLC_TCP_HOST"]), int(cfg["PLC_TCP_PORT"]))}
    for ch in ("ch1", "ch2"):
        out[f"{ch}.mfc"] = ("mfc", ch, *MFCD.endpoint(cfg, ch))
    for ch in ("ch1", "ch2"):
        out[f"{ch}.ig"] = ("ig", ch, *IGD.endpoint(cfg, ch))
    h, p, _t, _k = TSPD.endpoint(cfg)
    out["tsp"] = ("tsp", "shared", h, p)
    st = STM.settings(cfg, "ch2")
    out["ch2.stm100"] = ("stm100", "ch2", st.host, st.port)
    for ch in ("ch1", "ch2"):
        kind = pulse.get(ch, "none")
        if kind in ("dcpulse", "rfpulse"):
            out[f"{ch}.pulse"] = (kind, ch, *PP.endpoint(cfg, ch, kind))
    return out


def build(clock, cfg: Dict[str, Any], name: str, ep: Tuple[str, str, str, int], on_event) -> Probe:
    kind, ch, host, port = ep
    pr = Probe(name if kind not in ("dcpulse", "rfpulse") else f"{ch}.{kind}", kind, ch, host, int(port))

    def wrap(inner):
        pr.guard = G.GuardedTransport(inner, pr.name, "bench", cfg, on_event=on_event)
        pr.rec = RecordingTransport(pr.guard)
        return pr.rec

    if kind == "plc":
        inner = wrap(TcpTransport(clock, TcpSettings(host, int(port), float(cfg["PLC_TIMEOUT_S"]))))
        pr.outer = PLCD.make_transport(clock, cfg, inner=inner)
        pr.worker = PLCD.make_worker(pr.outer, cfg, False, on_event=on_event)
        pr.driver = PLCD.PlcDriver(clock, pr.worker, cfg, False, on_event=on_event)
        pr.blocks = tuple(PLCD.blocks())
        pr.source = (pr.driver.source_read, int(cfg["TICK_DEADLINE_MS"]))
    elif kind == "mfc":
        pr.outer = wrap(TcpTransport(clock, MFCD.tcp_settings(cfg, ch)))
        pr.worker = MFCD.make_worker(clock, pr.outer, cfg, ch, False, on_event=on_event)
        pr.driver = MFCD.MfcDriver(clock, pr.worker, cfg, False, on_event=on_event, chamber=ch)
        pr.blocks = tuple(MFCD.blocks(ch))
        pr.source = (pr.driver.source_read, MFCD.source_deadline_ms(cfg))
    elif kind == "ig":
        pr.outer = wrap(TcpTransport(clock, IGD.tcp_settings(cfg, ch)))
        pr.worker = IGD.make_worker(clock, pr.outer, cfg, ch, False, on_event=on_event)
        pr.driver = IGD.IgDriver(clock, pr.worker, cfg, False, on_event=on_event, chamber=ch)
        pr.blocks = tuple(IGD.blocks(ch))
        pr.source = (pr.driver.source_read, IGD.source_deadline_ms(cfg))
    elif kind == "tsp":
        _h, _p, ct, ka = TSPD.endpoint(cfg)
        inner = wrap(TcpTransport(clock, TcpSettings(host, int(port), ct), socket_factory=TSPD.socket_factory(ka)))
        pr.outer = TSPD.OnDemandTransport(clock, lambda _k: inner, None)
        pr.worker = Worker("tsp", pr.outer, writes_allowed=False, on_event=on_event)
        pr.driver = TSPD.TspDriver(clock, pr.worker, cfg, False, on_event=on_event)
        pr.blocks = tuple(TSPD.blocks())
    elif kind == "stm100":
        pr.outer = wrap(STM.make_transport(clock, cfg, ch))
        pr.worker = Worker(f"{ch}.stm100", pr.outer, writes_allowed=False, on_event=on_event, **STM.worker_kwargs())
        pr.driver = STM.Stm100Driver(clock, pr.worker, cfg, False, on_event=on_event, chamber=ch)
        pr.blocks = tuple(STM.blocks(ch))
    else:                                              # dcpulse / rfpulse
        pr.outer = wrap(PP.tcp_transport_factory(clock, cfg)(kind, ch))
        mod = DCP if kind == "dcpulse" else RFP
        cls = DCP.DcPulseDriver if kind == "dcpulse" else RFP.RfPulseDriver
        pr.driver = cls(clock, None, cfg, False, on_event=on_event, chamber=ch)
        pr.worker = Worker(pr.name, pr.outer, writes_allowed=False, on_event=on_event, **pr.driver.worker_params())
        pr.driver.bind(pr.worker)
        pr.blocks = tuple(mod.blocks(ch))
        fn = _dcp_tick(pr) if kind == "dcpulse" else _rfp_tick(pr, cfg)
        pr.source = (fn, int(cfg["TICK_DEADLINE_MS"]))
    return pr


def open_link(pr: Probe) -> None:
    """보호된 열기 한 번(재시도 없음 — 실패면 TransportError). TSP 는 실제 소켓까지 연다."""
    if pr.kind == "tsp":
        pr.outer.ensure_open()
    else:
        pr.outer.open()


# ═════════════ 장비별 질의 한 번(worker 스레드 안 — 요청 fn) ═════════════
def q_mfc(pr: Probe, cmd: str) -> Callable:
    d = pr.driver

    def fn(s):
        try:
            return d.io_query(s, Query(cmd, d.ms("MFC_TIMEOUT", 2000), 0.0, (), 0, "[BENCH]"))
        except ReadTimeout:
            return None                                  # 무응답 — 설정 읽기에서는 끊지 않고 다음 질의로
    return fn


def q_ig(pr: Probe, cmd: str = "RDI") -> Callable:
    d = pr.driver

    def fn(s):
        try:
            return d.io_query(s, Query(cmd, d.ms("IG_TIMEOUT_MS", 3000), 0.0, (), 0, "[BENCH]"))
        except ReadTimeout:
            return None
    return fn


def q_dcp(pr: Probe, code: int) -> Callable:
    def fn(s):
        try:
            tok = pr.driver._q(s, code)
        except ReadTimeout:
            return None
        return tok.hex(" ") if tok else None
    return fn


def q_rfp(pr: Probe, cmd: int, tag: str = "[BENCH]") -> Callable:
    d = pr.driver

    def fn(s):
        gap = d._gap_s()
        tmo = d._ms("QUERY_TIMEOUT_MS", 4500)
        until = min(s.deadline, max(d._ready_at, d.clock.monotonic()) + tmo + 0.1)
        try:
            payload = d._transact(s, cmd, b"", tmo, until, tag, False)
        except (ReadTimeout, RFP.RfpNak):
            payload = None
        d._ready_at = d.clock.monotonic() + gap
        return None if payload is None else payload.hex(" ")
    return fn


def q_tsp(pr: Probe) -> Callable:
    d = pr.driver

    def fn(s):
        try:
            return d._guarded(s, lambda: d._get_status(s))
        except TSPD.TspError:
            return None
    return fn


def q_stm(pr: Probe, cmd: str) -> Callable:
    d = pr.driver

    def fn(s):
        try:
            return d._query(s, cmd)
        except STM.StmError as e:
            return f"({type(e).__name__}: {e})"
    return fn


def queries(pr: Probe) -> List[Tuple[str, Callable, str]]:
    """설정 전부 읽기 — (질의 이름, fn, 뜻). 가드의 bench 목록 그대로(한 번씩)."""
    k = pr.kind
    if k == "mfc":
        return [(q, q_mfc(pr, q), "") for q in G.MFC_BENCH]
    if k == "ig":
        return [(q, q_ig(pr, q), "압력(Torr) 또는 IG OFF") for q in G.IG_BENCH]
    if k == "dcpulse":
        return [(f"0x{c:02X}", q_dcp(pr, c), DP.READ_LABELS.get(c, "")) for c in sorted(G.DCP_BENCH)]
    if k == "rfpulse":
        return [(str(c), q_rfp(pr, c), RFP.CMD_NAMES.get(c, RFP_QUERY_NAMES.get(c, ""))) for c in sorted(G.RFP_BENCH)]
    if k == "tsp":
        return [("WIN 205", q_tsp(pr), "0 STOP·1 FAIL·2 WAIT_INTERLOCK·3 RAMP·4 WAIT_SUBLIM·5 SUBLIM")]
    if k == "stm100":
        return [(q, q_stm(pr, q), "") for q in G.STM_BENCH]
    return []


# ═════════════ 펄스 장비 1초 읽기(드라이버는 출력 ON 때만 읽으므로 bench 전용 — 질의만) ═════════════
def _dcp_tick(pr: Probe) -> Callable:
    def read(s, seq):
        d, b = pr.driver, pr.driver.block
        out: Dict[str, Any] = {}
        raw = DP.piv_raw(d._q(s, DP.R_PIV))
        if raw is not None:
            e = d._scale(raw)
            out.update({f"{b}.p_w": e["P_W"], f"{b}.v_v": e["V_V"], f"{b}.i_a": e["I_A"]})
        for code, key in ((DP.R_ARC_SOFT, "arc_soft"), (DP.R_ARC_HARD, "arc_hard")):
            if d._room(s):
                v = DP.u16_tail(d._q(s, code))
                if v is not None:
                    out[f"{b}.{key}"] = v
        if d._room(s):
            st = DP.u16_tail(d._q(s, DP.R_STATUS))
            out[f"{b}.out_on"] = None if st is None else bool(st & 1)
        return out
    return read


def _rfp_tick(pr: Probe, cfg: Dict[str, Any]) -> Callable:
    order = (RFP.Q_STATUS, RFP.Q_FWD, RFP.Q_REF)

    def read(s, seq):
        d, b = pr.driver, pr.driver.block
        if d._ready_at > s.deadline - 0.15:
            return {}                                     # 간격(1.5 s)을 못 맞춤 — 이번 tick 은 안 읽음(stale)
        i = pr.state.get("rfp_i", 0)
        cmd = order[i % len(order)]
        h = q_rfp(pr, cmd, "[POLL BENCH]")(s)
        pr.state["rfp_i"] = i + 1
        if h is None:
            raise ReadTimeout()
        data = bytes.fromhex(h)
        if cmd == RFP.Q_STATUS:
            st = parse_status(data)
            if st is None:
                return {}
            return {f"{b}.out_on": st["on"], f"{b}.itl_open": st["ilock"], f"{b}.status_raw": st["raw"].hex(" ")}
        if len(data) < 2:
            return {}
        return {f"{b}.{'fwd_w' if cmd == RFP.Q_FWD else 'ref_w'}": float(aebus.u16(data))}
    return read

"""bench 도구(v2/tools/bench_probe.py) — 시뮬레이터를 127.0.0.1 TCP 서버로 띄워(SimTcpServer) 실제 소켓·실제 시계로 짧게 돌린다.

실제 장비망(192.168.1.x)에는 붙지 않는다 — 모든 주소를 127.0.0.1 로 바꾸고, 그렇지 않으면 시험이 먼저 멈춘다.
장비에 나간 바이트를 장비별로 검사해 '쓰기 0'(질의만)을 확인한다(가드 bench 목록 + 장비별 형식 + 시뮬레이터 기록 세 가지로).
"""
import json
import re
import socket
import time

import pytest

from v2.ch12 import cmd_guard as G
from v2.ch12.drivers import plc_map as PM
from v2.ch12.sim.chamber import Chamber
from v2.ch12.sim.dcpulse_sim import DcPulseSim
from v2.ch12.sim.ig_sim import IgSim
from v2.ch12.sim.mfc_sim import MfcSim
from v2.ch12.sim.plc_sim import PlcSim
from v2.ch12.sim.rfpulse_sim import RfPulseSim
from v2.ch12.sim.stm100_sim import Stm100Sim
from v2.ch12.sim.tcp_adapter import SimTcpServer
from v2.ch12.sim.tsp_sim import TspSim
from v2.tools import bench_probe as B

LOCAL = "127.0.0.1"
FAST = {"MFC_GAP_MS": 100, "MFC_POST_OPEN_QUIET_MS": 100, "IG_GAP_MS": 100, "CMD_GAP_MS": 100, "PLC_TIMEOUT_S": 0.5}
GUARD_NAME = {"plc": "plc", "ch1.mfc": "mfc", "ch2.mfc": "mfc", "ch1.ig": "ig", "ch2.ig": "ig", "tsp": "tsp",
              "ch2.stm100": "stm100", "ch1.rfpulse": "rfpulse", "ch2.dcpulse": "dcpulse"}


def _free_port() -> int:
    s = socket.socket()
    s.bind((LOCAL, 0))
    p = s.getsockname()[1]
    s.close()
    return p                                                      # 닫힌 포트 — 연결 거부


class Bench:
    def __init__(self, pulse=True):
        o = time.monotonic()
        c1, c2 = Chamber("ch1"), Chamber("ch2")
        self.sims = {"plc": PlcSim(c1, c2), "ch1.mfc": MfcSim(c1), "ch2.mfc": MfcSim(c2), "ch1.ig": IgSim(c1),
                     "ch2.ig": IgSim(c2), "tsp": TspSim(), "ch2.stm100": Stm100Sim(c2)}
        if pulse:
            self.sims["ch1.rfpulse"] = RfPulseSim(c1)
            self.sims["ch2.dcpulse"] = DcPulseSim(c2)
        self.srv = {k: SimTcpServer(s, o) for k, s in self.sims.items()}
        P = lambda k: self.srv[k].port if k in self.srv else _free_port()
        self.cfg = dict(FAST, PLC_TCP_HOST=LOCAL, PLC_TCP_PORT=P("plc"), MFC_TCP_HOST=LOCAL, CH1_MFC_TCP_PORT=P("ch1.mfc"),
                        CH2_MFC_TCP_PORT=P("ch2.mfc"), IG_TCP_HOST=LOCAL, CH1_IG_TCP_PORT=P("ch1.ig"),
                        CH2_IG_TCP_PORT=P("ch2.ig"), TSP_TCP_HOST=LOCAL, TSP_TCP_PORT=P("tsp"),
                        CH2_THICKNESS_TCP_HOST=LOCAL, CH2_THICKNESS_TCP_PORT=P("ch2.stm100"),
                        RFPULSE_TCP_HOST=LOCAL, CH1_RFPULSE_TCP_PORT=P("ch1.rfpulse"),
                        DCPULSE_TCP_HOST=LOCAL, CH2_DCPULSE_TCP_PORT=P("ch2.dcpulse"))
        self.pulse = {"ch1": "rfpulse", "ch2": "dcpulse"} if pulse else {}
        self.out = []

    def run(self, tmp_path, seconds=2, cfg=None, skip=()):
        over = dict(self.cfg, **(cfg or {}))
        for k, v in over.items():                                 # 장비망 주소가 하나도 남지 않게(실제 장비 접속 금지)
            assert not (isinstance(v, str) and v.startswith("192.168.")), k
        try:
            code = B.run(seconds, tmp_path, print_fn=self.out.append, cfg_over=over, skip=skip, pulse=self.pulse)
        finally:
            for s in self.srv.values():
                s.close()
        return code, "\n".join(self.out)

    def outside_bench(self):
        """장비에 닿은 바이트 중 bench 질의가 아닌 것 — 가드 판정과 장비별 형식 두 가지로."""
        bad = []
        for k, srv in self.srv.items():
            chk = G.checker(GUARD_NAME[k], "bench", _cfg_full(self.cfg))
            for w in srv.written:
                if k == "plc":
                    fmt = len(w) >= 8 and w[7] in (1, 3)
                elif k.endswith("mfc"):
                    fmt = w.startswith(b"R") and w.endswith(b"\r")
                elif k.endswith(".ig"):
                    fmt = w == b"RDI\r"
                elif k == "tsp":
                    fmt = len(w) > 6 and w[5:6] == b"0"
                elif k == "ch2.stm100":
                    fmt = w[2:-1].decode("ascii", "replace") in G.STM_BENCH
                elif k == "ch1.rfpulse":
                    fmt = len(w) >= 2 and w[1] >= 128
                else:
                    fmt = len(w) == 4                                # EP5 읽기 프레임(데이터 없음)
                if chk(w) or not fmt:
                    bad.append((k, w))
        return bad

    def sim_writes(self):
        out = list(self.sims["plc"].write_pdus())
        for k in ("ch1.mfc", "ch2.mfc"):
            out += self.sims[k].set_commands_seen()
        for k in ("ch1.ig", "ch2.ig"):
            out += [c for c in self.sims[k].commands_seen() if c != "RDI"]
        if "ch1.rfpulse" in self.sims:
            out += [c for c in self.sims["ch1.rfpulse"].cmds_seen() if c < 128]
            out += [c for c in self.sims["ch2.dcpulse"].cmds_seen() if c not in G.DCP_BENCH]
        return out


def _cfg_full(over):
    from v2.common.paramdef import defaults
    from v2.ch12.params import PARAMS
    return dict(defaults(PARAMS), **over)


def test_normal_run_reads_only_and_writes_summary(tmp_path):
    b = Bench()
    code, out = b.run(tmp_path, seconds=3)
    assert code == 0, out
    assert "읽기 전용" in out and "안전 확인 통과" in out and "읽기 전용 가드 켜짐(bench)" in out
    assert b.outside_bench() == [] and b.sim_writes() == []
    assert sum(len(s.written) for s in b.srv.values()) > 60                      # 읽기는 나갔다(헛시험 아님)
    folder = next(tmp_path.glob("v2_bench_*"))
    names = {p.name for p in folder.iterdir()}
    assert {"summary.txt", "events.jsonl", "inventory.json"} <= names
    assert {f"rec_{k}.jsonl" for k in GUARD_NAME} <= names
    assert any(n.startswith("snap_") and n.endswith(".sqlite") for n in names) and any(n.endswith(".csv") for n in names)
    s = (folder / "summary.txt").read_text(encoding="utf-8")
    for want in ("[ch1.mfc] 15개", "[ch2.mfc] 15개", "[ch1.ig] 1개", "[tsp] 1개", "[ch2.stm100] 10개", "[ch1.rfpulse] 13개",
                 "[ch2.dcpulse] 15개", "가드가 막은 명령: 0건", "놓친 tick", "품질 plc", "품질 ch2.dcpulse", "응답 plc",
                 "간격 최소", "[미확인]", "연결 기록", "'Q0+0.0+0.0+0.0+0.0'", "'IG OFF'"):
        assert want in s, want
    assert all(m == "0" for m in re.findall(r"무응답 (\d+)", s)), s
    inv = json.loads((folder / "inventory.json").read_text(encoding="utf-8"))["items"]
    assert any(i["device"] == "plc" and i["meaning"] == "SW_RF_SELECT" for i in inv)
    assert any(i["device"] == "ch2.dcpulse" and i["query"] == "0x9C" and i["raw"] == "9c 00 01" for i in inv)
    ev = [json.loads(x) for x in (folder / "events.jsonl").read_text(encoding="utf-8").splitlines()]
    assert ev and not any(e.get("kind") == "COMMAND_BLOCKED" for e in ev)


def test_connection_failure_stops_without_retry_or_writes(tmp_path):
    b = Bench(pulse=False)
    code, out = b.run(tmp_path, cfg={"CH2_IG_TCP_PORT": _free_port()})
    assert code == 3 and "ch2.ig" in out and "연결할 수 없음" in out and "다시 시도하지 않고" in out
    assert all(not s.written for s in b.srv.values())                            # 아무것도 보내지 않음
    assert "수집 시작" not in out and (next(tmp_path.glob("v2_bench_*")) / "summary.txt").exists()


def test_heater_run_on_stops_with_zero_writes(tmp_path):
    b = Bench(pulse=False)
    b.sims["plc"].coils[640] = True                                             # M00400 히터 RUN
    code, out = b.run(tmp_path)
    assert code == 2 and "히터 RUN(M00400)" in out and "수집 시작" not in out
    assert b.outside_bench() == [] and b.sim_writes() == []


def test_dac_trace_stops_and_dcv_set_is_only_note(tmp_path):
    b = Bench(pulse=False)
    b.sims["plc"].regs[PM.REG_MAP["DCV_WRITE_2"]] = 40                         # D6 — CH2 RF D/A 설정이 남아 있음
    code, out = b.run(tmp_path)
    assert code == 2 and "D/A 출력 설정이 남아 있음(D6=40)" in out and b.outside_bench() == []
    assert b.sims["plc"].regs[PM.REG_MAP["DCV_WRITE_2"]] == 40                  # 끄지도 않는다(쓰기 0)


def test_plc_unreadable_stops(tmp_path):
    b = Bench(pulse=False)
    b.sims["plc"].add_fault("no_response", 0.0, 1e9)
    code, out = b.run(tmp_path)
    assert code == 2 and "PLC 를 읽을 수 없음" in out and "수집 시작" not in out and b.sim_writes() == []


def test_out_folder_in_operational_dir_refused(tmp_path):
    b = Bench(pulse=False)
    code, out = b.run(tmp_path / "logs", cfg={"LOG_ROOT_DIR": str(tmp_path / "logs")})
    assert code == 4 and "기록 폴더 거부" in out
    assert all(s.connections == 0 for s in b.srv.values())                     # 장비에 연결하지도 않음
    assert B.out_refused(B.Path(r"\\VanaM_NAS\x"), {}) and B.out_refused(B.Path("G:/공유 드라이브/x"), {})


def test_pulse_not_chosen_is_not_connected(tmp_path):
    b = Bench(pulse=False)
    code, out = b.run(tmp_path, skip=("ch2.ig",))
    assert code == 0 and "ch1.pulse 제외(--ch1-pulse none" in out and "ch2.ig 제외(--skip)" in out
    assert b.srv["ch2.ig"].connections == 0 and b.outside_bench() == []


def test_cli_is_bench_only():
    with pytest.raises(SystemExit):
        B.main(["--mode", "prod"])                                              # 모드 선택지가 없다(bench 고정)
    with pytest.raises(SystemExit):
        B.main(["--set", "RUN_MODE=prod"])

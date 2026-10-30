"""공정 엔진 시험 공통 — Ch12Rig(시뮬레이터 + 실제 드라이버 + 가상 시계)에 챔버 엔진을 꽂고 끝날 때까지 돌린다.

- make(tmp, cfg): CH1 RF Pulse·CH2 DC Pulse·CH2 RF 연속도 켠 설정(골든 시나리오가 쓰는 장비), MFC 시뮬레이터 SP3·SP4
  setpoint 를 작업 압력으로(v1 은 SP3/SP4 setpoint 를 장비에 미리 넣어 두고 읽는다 — use_sp_target).
- run(rig, until, max_ticks, hooks): tick 마다 hooks(t) → until() 이 참이면 멈춤.
- mfc_prefixes·plc_writes: 스텝 → 시뮬레이터가 받아야 할 명령(순서 비교용).
"""
from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict, List, Optional

from v2.common.clock import VirtualClock
from v2.ch12.drivers import plc_map as PM
from v2.ch12.process_engine import ProcessEngine
from v2.ch12.process_steps import norm_plc_name
from v2.ch12.sim.rig import Ch12Rig

CFG = {"CH1_SUPPORTS_RFPULSE": True, "CH2_SUPPORTS_DCPULSE": True, "CH2_SUPPORTS_RF_CONT": True,
       "CAMERA_MIN_FREE_GB": 0.0}


def make(tmp, cfg: Optional[dict] = None, chambers=("ch1", "ch2"), sp=(3.0, 5.0), **eng_kw):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = Ch12Rig(c, Path(tmp), cfg=dict(CFG, **(cfg or {})))
    rig.sims["ch1.mfc"].sp[2] = sp[0] * 0.1            # SP3(CH1) — UI mTorr × MFC_PRESSURE_SCALE 0.1
    rig.sims["ch2.mfc"].sp[3] = sp[1] * 0.1            # SP4(CH2)
    rig.eng = {}
    for ch in chambers:
        e = ProcessEngine(rig, ch, excel_main_dir=Path(tmp) / "xl_main", **eng_kw.get(ch, {}))
        rig.register_engine(f"proc.{ch}", e)
        rig.eng[ch] = e
    return rig


def run(rig, until: Callable[[], bool], max_ticks: int = 3000, hooks: Optional[List[Callable]] = None,
        start: bool = True, stop: bool = True) -> int:
    for h in hooks or ():
        rig.loop.tick_hooks.append(h)
    if start:
        assert rig.start()
    n = 0
    try:
        while n < max_ticks:
            rig.loop.run(5)
            n += 5
            if until():
                break
    finally:
        if stop:
            rig.stop()
            rig.clock.close()
    return n


def idle(*engines) -> Callable[[], bool]:
    return lambda: all(not e.busy for e in engines)


def at(seq: int, fn) -> Callable:
    def hook(t):
        if t.seq == seq:
            fn()
    return hook


def when(pred, fn) -> Callable:
    """pred() 가 처음 참이 되는 tick 에 fn() 한 번."""
    done = [False]

    def hook(t):
        if not done[0] and pred():
            done[0] = True
            fn()
    return hook


def ends(e) -> List[tuple]:
    return [(x.data.get("result"), x.code) for x in e.events if x.kind == "run_end"]


def logs(e) -> List[str]:
    return list(e.log_lines)


def has_log(e, text: str) -> bool:
    return any(text in l for l in e.log_lines)


_MFC = {"FLOW_OFF": lambda a: f"L{a['channel']}0", "VALVE_OPEN": lambda a: "O", "PS_ZEROING": lambda a: "Z1",
        "MFC_ZEROING": lambda a: f"L{4 + a['channel']}1", "FLOW_SET": lambda a: f"Q{a['channel']} ",
        "FLOW_ON": lambda a: f"L{a['channel']}1"}


def mfc_prefixes(steps) -> List[str]:
    out = []
    for s in steps:
        if s.action != "MFC_CMD":
            continue
        cmd, a = s.params
        if cmd in _MFC:
            out.append(_MFC[cmd](a))
        elif cmd.endswith("_ON") and cmd.startswith("SP"):
            out.append(f"D{cmd[2]}")
        elif cmd.endswith("_SET") and cmd.startswith("SP"):
            out.append(f"S{cmd[2]} ")
    return out


def plc_name(nname: str, n: int) -> Optional[str]:
    if n == 1 and nname in ("G1", "G2", "G3", "N2"):
        return None
    if nname == "MV":
        return f"MAIN_{n}_GAS_SW"
    if nname == "AR":
        return f"Ar_{n}_GAS_SW"
    if nname in ("O2", "N2"):
        return f"{nname}_{n}_GAS_SW"
    if nname == "MS":
        return f"MAIN_SHUTTER_{n}_SW"
    if nname in ("G1", "G2", "G3"):
        return f"SHUTTER_{nname[1]}_SW"
    return nname


def plc_expected(steps, n: int) -> List[tuple]:
    out = []
    for s in steps:
        if s.action == "PLC_CMD":
            nm = plc_name(norm_plc_name(s.params[0]), n)
            if nm is not None:
                out.append((PM.resolve(nm), bool(s.params[1])))
    return out


def plc_writes(sim) -> List[tuple]:
    out = []
    for p in sim.write_pdus():
        if p[0] == 5:
            out.append((int.from_bytes(p[1:3], "big"), p[3:5] == b"\xff\x00"))
    return out


def subsequence(want: list, got: list) -> bool:
    it = iter(got)
    return all(any(g == w for g in it) for w in want)


def prefix_subsequence(want: List[str], got: List[str]) -> bool:
    it = iter(got)
    return all(any(g.startswith(w) for g in it) for w in want)

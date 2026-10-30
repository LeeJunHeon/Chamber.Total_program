"""챔버12 MFC 시뮬레이터 — MOXA NPort 뒤 MFC 컨트롤러(ASCII 명령 + CR, 값은 %FS). 챔버K v2/chk/sim/mfc_sim.py(de8e946)의 모델을 쓰고
챔버12 v1 이 보내는 명령(개별 L{ch}1/L{ch}0, Q{ch} 값, S{n}, D{n}, O, C, Z1, L{4+ch}1)에 맞췄다. 응답 형식은 v1 파서가 읽는 모양(가정):
  R60 → "Q0+aa.a+bb.b+cc.c+dd.d"(채널 유량 %FS) · R61~R64 → "Q{n}+x.x" · R65~R68 → "Q{n}+xxx.xx"(설정)
  R69 → "L0 bbbb" · R5 → "P+x.xx"(압력 = mTorr × 0.1) · R1~R4 → "S{n}+xxx.xx" · R6 → "V+vvv.v" · R7 → "M{선택}{Y}{Z}"
  줄 끝 CR LF. 설정 명령은 응답 없음(echo=True 면 받은 명령을 그대로 한 줄 되돌림 — v1 MFC_SKIP_ECHO 시험용).
유량: 켜진 채널은 설정 %FS 쪽으로 1차 지연(flow_tau_s, 0 이면 즉시) → 공용 챔버 gas_sccm(ch1 ar·ch2 o2·ch3 n2).
  sccm = %FS ÷ 스케일(v1 MFC_SCALE_FACTORS — CH2 O2 10 이면 %FS 100 = 10 sccm).
압력 제어(가정 모델): D{n} → 그 설정점으로 밸브 제어, O 열림, C 닫힘.
고장 주입: SimDevice 공통(no_response·delay·disconnect·garbage) + ignore_writes(접두어 쓰기 무시) + echo + reply_prefix_noise
  (응답 앞에 다른 줄 하나 — 접두사 거르기 시험).
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional

from v2.common.simdev import SimDevice, sim_log
from v2.ch12.sim.chamber import BASE_TORR, THROTTLE_MIN, TORR_PER_SCCM, Chamber

QUERY_LATENCY_S = 0.03
BAUD = 9600
FLOW_TAU_S = 1.0
VALVE_TAU_S = 2.0
VALVE_OPEN_TAU_S = 0.5
PRESSURE_SCALE = 0.1                 # 장비값 = mTorr × 0.1 (v1 MFC_PRESSURE_SCALE)
_GAS = {0: "ar", 1: "o2", 2: "n2"}


class MfcSim(SimDevice):
    def __init__(self, chamber: Optional[Chamber] = None, scale=(1.0, 1.0, 1.0, 1.0), flow_tau_s: float = FLOW_TAU_S,
                 name: str = "mfc") -> None:
        super().__init__(name, latency_s=0.0)
        self.chamber = chamber or Chamber()
        self.scale = list(scale) + [1.0] * (4 - len(scale))
        self.flow_tau_s = flow_tau_s
        self.set_pct = [0.0] * 4
        self.on = [False] * 4
        self.flow_pct = [0.0] * 4
        self.sp = [0.0] * 5
        self.sp_sel = 0
        self.valve_mode = "open"            # open / closed / hold / control
        self.valve = 1.0
        self.zeroed: List[str] = []
        self.t = self.chamber.t
        self.received = sim_log()
        self.ignore_writes: set = set()
        self.echo = False
        self.reply_prefix_noise: Optional[str] = None

    # ───────── 시각 ─────────
    def _advance(self, now: float) -> None:
        while self.t < now - 1e-12:
            dt = min(0.1, now - self.t)
            for i in range(4):
                tgt = self.set_pct[i] if self.on[i] else 0.0
                if self.flow_tau_s <= 0:
                    self.flow_pct[i] = tgt
                else:
                    self.flow_pct[i] += (tgt - self.flow_pct[i]) * min(1.0, dt / self.flow_tau_s)
            for i, g in _GAS.items():
                sf = self.scale[i] or 1.0
                self.chamber.gas_sccm[g] = self.flow_pct[i] / sf
            if self.valve_mode == "open":
                vt = 1.0
            elif self.valve_mode == "closed":
                vt = 0.0
            elif self.valve_mode == "control" and self.sp_sel:
                target_torr = self.sp[self.sp_sel - 1] / PRESSURE_SCALE / 1000.0
                flow = self.chamber.total_sccm()
                if target_torr <= BASE_TORR * 1.01 or flow <= 0:
                    vt = 1.0
                else:
                    eff = flow * TORR_PER_SCCM / (target_torr - BASE_TORR)
                    vt = (eff - THROTTLE_MIN) / (1.0 - THROTTLE_MIN)
                vt = min(1.0, max(0.0, vt))
            else:
                vt = self.valve
            tau = VALVE_OPEN_TAU_S if self.valve_mode in ("open", "closed") else VALVE_TAU_S
            self.valve += (vt - self.valve) * min(1.0, dt / tau)
            self.chamber.throttle = self.valve
            self.chamber.advance_to(self.t + dt)
            self.t += dt
        if self.flow_tau_s <= 0:                        # 즉시 모델: 마지막 명령도 바로 반영
            for i in range(4):
                self.flow_pct[i] = self.set_pct[i] if self.on[i] else 0.0
        self.chamber.advance_to(now)

    def pressure_hw(self) -> float:
        return self.chamber.pressure_mtorr * PRESSURE_SCALE

    # ───────── 통신 ─────────
    def respond(self, data: bytes, now: float):
        self._advance(now)
        self.received.append((now, data))
        if self._active("no_response", now):
            return []
        cmd = data.decode("ascii", errors="replace").strip()
        out = b""
        if self.echo:
            out += (cmd + "\r\n").encode()
        ans = self.handle(data, now)
        if ans is not None:
            if self.reply_prefix_noise:
                out += (self.reply_prefix_noise + "\r\n").encode()
            if self._active("garbage", now):
                ans = b"#" * max(1, len(ans) - 2) + ans[-2:]
            out += ans
        if not out:
            return []
        lat = QUERY_LATENCY_S + (len(data) + len(out)) * 10.0 / BAUD + sum(f.extra_s for f in self._active("delay", now))
        return [(now + lat, out)]

    def handle(self, data: bytes, now: float) -> Optional[bytes]:
        cmd = data.decode("ascii", errors="replace").strip()
        if cmd.startswith("R"):
            ans = self._query(cmd)
            return None if ans is None else (ans + "\r\n").encode()
        if any(cmd.startswith(p) for p in self.ignore_writes):
            return None
        self._set(cmd)
        if self.flow_tau_s <= 0:
            self._advance(now)
        return None

    def _query(self, cmd: str) -> Optional[str]:
        try:
            n = int(cmd[1:])
        except ValueError:
            return None
        if n == 60:
            return "Q0" + "".join(f"{v:+.1f}" for v in self.flow_pct)
        if 61 <= n <= 64:
            return f"Q{n - 60}{self.flow_pct[n - 61]:+.1f}"
        if 65 <= n <= 68:
            return f"Q{n - 60}{self.set_pct[n - 65]:+07.2f}"
        if n == 69:
            return "L0 " + "".join("1" if x else "0" for x in self.on)
        if n == 5:
            return f"P{self.pressure_hw():+.2f}"
        if 1 <= n <= 4:
            return f"S{n}{self.sp[n - 1]:+07.2f}"
        if n == 6:
            return f"V{self.valve * 100:+.1f}"
        if n == 7:
            y = {"control": 0, "open": 2, "closed": 4, "hold": 0}[self.valve_mode]
            return f"M{self.sp_sel}{y}{1 if self.pressure_hw() > 10 else 0}"
        return None

    def _set(self, cmd: str) -> None:
        m = re.fullmatch(r"Q([1-4])\s+([+\-]?\d+(?:\.\d+)?(?:e[+\-]?\d+)?)", cmd)
        if m:
            self.set_pct[int(m.group(1)) - 1] = round(float(m.group(2)), 2)
            return
        m = re.fullmatch(r"L0\s*([01]{4})", cmd)
        if m:
            self.on = [c == "1" for c in m.group(1)]
            return
        m = re.fullmatch(r"L([1-4])([01])", cmd)
        if m:
            self.on[int(m.group(1)) - 1] = m.group(2) == "1"
            return
        m = re.fullmatch(r"L([5-8])1", cmd)
        if m or cmd == "Z1":
            self.zeroed.append(cmd)
            return
        m = re.fullmatch(r"S([1-5])\s+([+\-]?\d+(?:\.\d+)?)", cmd)
        if m:
            self.sp[int(m.group(1)) - 1] = float(m.group(2))
            return
        m = re.fullmatch(r"D([1-5])", cmd)
        if m:
            self.sp_sel = int(m.group(1))
            self.valve_mode = "control"
            return
        if cmd == "O":
            self.valve_mode, self.sp_sel = "open", 0
        elif cmd == "C":
            self.valve_mode, self.sp_sel = "closed", 0
        elif cmd == "H":
            self.valve_mode = "hold"

    def commands_seen(self) -> List[str]:
        return [d.decode("ascii", errors="replace") for _, d in self.received]

    def set_commands_seen(self) -> List[str]:
        return [c.strip() for c in self.commands_seen() if not c.strip().startswith("R")]

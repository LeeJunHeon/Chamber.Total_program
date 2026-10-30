"""챔버12 이온 게이지 시뮬레이터 — MOXA NPort 뒤 IG 컨트롤러(ASCII 명령 + CR). v1 이 보내는 세 명령만 안다.

  'SIG 1' → "OK"(점등 — ignite_s 뒤 측정 시작), 'SIG 0' → "OK"(소등 — v1 은 이 응답을 기다리지 않고 흡수한다)
  'RDI'   → 점등 중이면 압력 "m.mx10e-e"(v1 예 '1.2x10e-5'), 아니면 "IG OFF"
  그 밖   → "ERR"
응답 형식·지연은 가정값(실제 장비 사실 아님 — bench 에서 확인). 줄 끝 CR LF. 압력은 공용 챔버 모델(v2/ch12/sim/chamber.py)에서.
고장 주입: SimDevice 공통(no_response·delay·disconnect·garbage) + sig1_reply(점등 응답 바꾸기, 예 "ERR") +
  trip_at(이 시각에 저절로 꺼짐 — 'IG OFF' 재점등 시험) + trip_torr(이 압력 위에서 점등 중이면 꺼짐) + echo(받은 명령을 한 줄 되돌림) +
  rdi_script(RDI 응답을 이 목록 차례로 — 마지막 값 반복, v1 골든 가짜 장비와 같은 응답을 낼 때).
"""
from __future__ import annotations

import math
from typing import List, Optional

from v2.common.simdev import SimDevice, sim_log
from v2.ch12.sim.chamber import Chamber

LATENCY_S = 0.05
IGNITE_S = 0.5
TRIP_TORR = 1.0e-2


def fmt_rdi(torr: float) -> str:
    """1.2x10e-5 꼴(가수 소수 1자리)."""
    if torr <= 0:
        return "0.0x10e0"
    e = math.floor(math.log10(torr))
    m = torr / 10 ** e
    if round(m, 1) >= 10.0:
        m, e = m / 10.0, e + 1
    return f"{m:.1f}x10e{e}"


class IgSim(SimDevice):
    def __init__(self, chamber: Optional[Chamber] = None, name: str = "ig") -> None:
        super().__init__(name, latency_s=LATENCY_S)
        self.chamber = chamber or Chamber()
        self.on = False
        self.on_since: Optional[float] = None
        self.received = sim_log()
        self.sig1_reply = "OK"
        self.trip_at: List[float] = []
        self.trip_torr = TRIP_TORR
        self.echo = False
        self.rdi_script: Optional[List[str]] = None

    def _advance(self, now: float) -> None:
        self.chamber.advance_to(now)
        if self.on and self.trip_at and now >= self.trip_at[0]:
            self.trip_at.pop(0)
            self.on, self.on_since = False, None
        if self.on and self.chamber.pressure > self.trip_torr:
            self.on, self.on_since = False, None

    def measuring(self, now: float) -> bool:
        return self.on and self.on_since is not None and now - self.on_since >= IGNITE_S

    def respond(self, data: bytes, now: float):
        self._advance(now)
        self.received.append((now, data))
        out = super().respond(data, now)
        if self.echo and not self._active("no_response", now):
            cmd = data.decode("ascii", errors="replace").strip()
            out = [(now + 0.01, (cmd + "\r\n").encode())] + out
        return out

    def handle(self, data: bytes, now: float) -> Optional[bytes]:
        cmd = data.decode("ascii", errors="replace").strip()
        if cmd == "SIG 1":
            if self.sig1_reply == "OK" and not self.on:
                self.on, self.on_since = True, now
            return (self.sig1_reply + "\r\n").encode() if self.sig1_reply else None
        if cmd == "SIG 0":
            self.on, self.on_since = False, None
            return b"OK\r\n"
        if cmd == "RDI":
            if self.rdi_script:
                v = self.rdi_script.pop(0) if len(self.rdi_script) > 1 else self.rdi_script[0]
                return (v + "\r\n").encode()
            if not self.measuring(now):
                return b"IG OFF\r\n"
            return (fmt_rdi(self.chamber.pressure) + "\r\n").encode()
        return b"ERR\r\n"

    def commands_seen(self) -> List[str]:
        return [d.decode("ascii", errors="replace").strip() for _, d in self.received]

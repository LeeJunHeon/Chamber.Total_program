"""두께 모니터 Sycon STM-100/MF 시뮬레이터 — Sycon protocol(v1 device/thickness_monitor.py 독스트링의 프레임). 값·지연은 가정값.

받는 것: STX LEN data CHK. 체크섬·길이가 틀리면 응답 없음(v1 독스트링: 장비는 아무 응답도 하지 않는다).
응답: STX LEN ('A'|'B' + 값) CHK. B 는 전원 리셋 플래그(power_cycle() 뒤 'L' 을 받을 때까지).
값은 공용 챔버 모델에서: S = (Chamber.thickness_A − 영점) 정수 Å, T = Chamber.rate_A_s(+NNN.N), U·V 는 두께로 줄어드는 가정값,
M = '@'(정상)·'!'(Chamber.xtal_fail). XTAL FAIL 중 S·T 는 '--------', U·V 는 '    . '(값 없음).
B/C = 영점(B 두께·타이머, C 두께 — 이 모델에서는 둘 다 두께 영점). E=/F=/J= 쓰기(범위 밖은 'H'), E?/F?/J? 되읽기,
L = 리셋 플래그 확인, @ = 판, a = 리셋 상태. 모르는 명령 'F', '?'/'=' 가 아닌 수식자 'J'.
장비 고유 이상: power_cycle()(리셋 플래그)·sticky_tooling(J= 를 받아도 되읽기가 1.0 다름 — 되읽기 불일치)·
  echo(값 앞에 명령 글자를 붙임 "U 5319234")·script(받은 프레임마다 정해진 응답 바이트 — 골든 비교용, None = 무응답).
통신 이상은 SimDevice 고장(no_response·delay·disconnect·garbage).
"""
from __future__ import annotations

from typing import List, Optional

from v2.common.simdev import SimDevice
from v2.ch12.sim.chamber import Chamber

STX = 0x02
LATENCY_S = 0.02              # 가정: 9600 bps 에서 짧은 프레임 + 장비 처리
FREQ_START_HZ = 6_000_000     # 가정: 새 크리스탈 6 MHz
FREQ_PER_A = 0.05             # 가정: 1 Å 당 주파수 감소(Hz)
LIFE_SPAN_HZ = 1_000_000      # 가정: 수명 100 % ↔ 주파수 1 MHz 감소


def checksum(data: bytes) -> int:
    return sum(data) & 0xFF


def frame(payload: str) -> bytes:
    d = payload.encode("ascii")
    return bytes([STX, len(d)]) + d + bytes([checksum(d)])


class Stm100Sim(SimDevice):
    def __init__(self, chamber: Optional[Chamber] = None, script: Optional[List] = None) -> None:
        super().__init__("stm100", latency_s=LATENCY_S)
        self.chamber = chamber or Chamber("ch2")
        self.zero_A = 0.0
        self.density, self.z_factor, self.tooling = 8.9, 1.0, 100.0
        self.reset_flag = False
        self.sticky_tooling = False
        self.echo = False
        self.script: Optional[List] = list(script) if script is not None else None
        self.cmds: List[str] = []          # 받은 명령(시험용)

    def power_cycle(self) -> None:
        self.reset_flag = True

    # ───────── 값 ─────────
    def thickness_A(self) -> float:
        return self.chamber.thickness_A - self.zero_A

    def freq_hz(self) -> int:
        return int(FREQ_START_HZ - FREQ_PER_A * self.chamber.thickness_A)

    def life_pct(self) -> float:
        used = (FREQ_START_HZ - self.freq_hz()) / LIFE_SPAN_HZ * 100.0
        return max(0.0, min(100.0, 100.0 - used))

    # ───────── 통신 ─────────
    def respond(self, data: bytes, now: float):
        if self.script is not None:
            self.rx_log.append((now, data))
            self.cmds.append(self._decode(data) or data.hex())
            if not self.script:
                return []
            r = self.script.pop(0)
            return [] if r is None else [(now + self.latency_s, r)]
        return super().respond(data, now)

    @staticmethod
    def _decode(data: bytes) -> Optional[str]:
        if len(data) < 3 or data[0] != STX or data[1] != len(data) - 3:
            return None
        d = data[2:-1]
        if checksum(d) != data[-1]:
            return None
        return d.decode("ascii", "replace")

    def handle(self, data: bytes, now: float) -> Optional[bytes]:
        cmd = self._decode(data)
        if cmd is None:
            return None                                    # 체크섬·길이 오류 — 응답 없음
        self.cmds.append(cmd)
        self.chamber.advance_to(now)
        ok = "B" if self.reset_flag else "A"
        bad_cmd, bad_val, bad_mod = ("G", "I", "K") if self.reset_flag else ("F", "H", "J")
        xf = self.chamber.xtal_fail
        c = cmd[:1]
        echo = (c + " ") if self.echo else ""
        if cmd == "S":
            return frame(ok + ("--------" if xf else echo + f"{int(round(self.thickness_A()))}"))
        if cmd == "T":
            return frame(ok + ("--------" if xf else echo + f"{self.chamber.rate_A_s:+06.1f}"))
        if cmd == "U":
            return frame(ok + ("    . " if xf else echo + f"{self.freq_hz()}"))
        if cmd == "V":
            return frame(ok + ("    . " if xf else echo + f"{self.life_pct():.1f}"))
        if cmd == "M":
            return frame(ok + echo + ("!" if xf else "@"))
        if cmd == "L":
            self.reset_flag = False
            return frame("A")
        if cmd in ("B", "C"):
            self.zero_A = self.chamber.thickness_A
            return frame(ok)
        if cmd == "@":
            return frame(ok + "STM-100 SIM")
        if cmd == "a":
            return frame(ok + ("1" if self.reset_flag else "0"))
        if c in ("E", "F", "J") and len(cmd) >= 2:
            mod, arg = cmd[1], cmd[2:]
            if mod == "?":
                v = {"E": self.density, "F": self.z_factor, "J": self.tooling}[c]
                if c == "J" and self.sticky_tooling:
                    v += 1.0
                return frame(ok + echo + (f"{v:.1f}" if c == "J" else f"{v:.3f}"))
            if mod == "=":
                try:
                    v = float(arg)
                except ValueError:
                    return frame(bad_val)
                lo, hi = {"E": (0.5, 99.99), "F": (0.1, 9.999), "J": (10.0, 399.0)}[c]
                if not (lo <= v <= hi):
                    return frame(bad_val)
                if c == "E":
                    self.density = v
                elif c == "F":
                    self.z_factor = v
                else:
                    self.tooling = v
                return frame(ok)
            return frame(bad_mod)
        return frame(bad_cmd)

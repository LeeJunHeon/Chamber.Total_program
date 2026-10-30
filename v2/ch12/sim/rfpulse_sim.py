"""RF Pulse 시뮬레이터(챔버12) — AE CESAR, AE Bus(주소 RFPULSE_ADDR 1). 챔버K v2/chk/sim/cesar_sim.py 를 따랐다.

명령(v1 device/rf_pulse.py 가 쓰는 것): 1 RF OFF, 2 RF ON, 3 조절 모드(6 FWD/7 LOAD/8 EXT), 8 셋포인트 W(u16), 14 제어 모드
  (2 Host 등), 27 펄싱(0~5), 93 펄스 주파수 Hz(u24), 96 듀티 %(u16), 155 제어 모드, 162 상태 4 B, 165 순방향 W, 166 반사 W,
  193 주파수, 196 듀티. 받으면 ACK 06 뒤 프레임(설정 명령은 CSR 1 B). 체크섬이 틀리면 NAK 15, 주소가 다르면 무응답.
CSR(가정 포함): Host 가 아니면 설정 명령에 1, RF ON 중 3 에 2(14 로 모드를 바꾸면 출력 꺼짐 — 매뉴얼), 범위 밖 4·50·51,
  인터록 열림이면 RF ON 에 5. 응답 지연 = PROCESS_S + (요청 + ACK + 응답) 바이트 × 11 비트 / 9600(8O1 가정).
플라즈마(가정 모델): RF ON 이면 순방향 = 셋포인트(1차 지연 0.3 s), 반사 = 순방향 × (점화 0.02 / 점화 전 0.6) + ref_inject_w.
  공용 챔버의 rfp_w 에 순방향을 넣는다.
시험용: csr_once{명령: CSR}(한 번만 그 CSR — 목록이면 차례로), readback_offset(193 되읽기를 틀리게), interlock_open, fwd_scale.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Union

from v2.ch12.sim.chamber import Chamber
from v2.common.aebus import ACK, NAK, build, le, u16, u24, xor
from v2.common.simdev import SimDevice, sim_log

PROCESS_S = 0.02
BIT_S = 11.0 / 9600.0
FWD_TAU_S = 0.3
REF_RATIO_LIT = 0.02
REF_RATIO_UNLIT = 0.6


class RfPulseSim(SimDevice):
    def __init__(self, chamber: Optional[Chamber] = None, addr: int = 1, control: int = 6) -> None:
        super().__init__("rfpulse", latency_s=0.0)
        self.chamber = chamber or Chamber()
        self.addr = addr
        self.control = control
        self.reg_mode = 6
        self.setpoint = 0
        self.pulsing = 0
        self.freq = 1000
        self.duty = 50
        self.out = False
        self.fwd = 0.0
        self.fwd_scale = 1.0
        self.interlock_open = False
        self.ref_inject_w = 0.0
        self.csr_once: Dict[int, Union[int, List[int]]] = {}
        self.readback_offset = 0
        self.t = self.chamber.t
        self.received = sim_log()

    def _advance(self, now: float) -> None:
        self.chamber.advance_to(now)
        while self.t < now - 1e-12:
            dt = min(0.1, now - self.t)
            if self.interlock_open:
                self.out = False
            tgt = float(self.setpoint) * self.fwd_scale if self.out else 0.0
            self.fwd += (tgt - self.fwd) * min(1.0, dt / FWD_TAU_S)
            self.t += dt
        self.chamber.rfp_w = self.fwd if self.out else 0.0

    def reflected(self) -> float:
        if self.fwd < 0.5:
            return 0.0
        ratio = REF_RATIO_LIT if self.chamber.can_ignite() else REF_RATIO_UNLIT
        return self.fwd * ratio + self.ref_inject_w

    def status(self) -> bytes:
        b1 = (0x60 if self.out else 0) | (0x80 if self.out and abs(self.fwd - self.setpoint) > 5 else 0)
        return bytes([b1, 0x80 if self.interlock_open else 0, 0, 0])

    def respond(self, data: bytes, now: float):
        self._advance(now)
        self.received.append((now, data))
        if self._active("no_response", now):
            return []
        if len(data) < 3 or (data[0] >> 3) != self.addr:
            return []
        if xor(data) != 0:
            return [(now + PROCESS_S + (len(data) + 1) * BIT_S, bytes([NAK]))]
        cmd, n = data[1], data[0] & 0x07
        payload = data[3:3 + data[2]] if n == 7 else data[2:2 + n]
        frame = bytes([ACK]) + build(self.addr, cmd, self._handle(cmd, payload))
        if self._active("garbage", now):
            frame = frame[:-1] + bytes([frame[-1] ^ 0x5A])
        lat = PROCESS_S + (len(data) + len(frame)) * BIT_S + sum(f.extra_s for f in self._active("delay", now))
        return [(now + lat, frame)]

    def _handle(self, cmd: int, d: bytes) -> bytes:
        if cmd in self.csr_once:
            v = self.csr_once[cmd]
            if isinstance(v, list):
                c = v.pop(0)
                if not v:
                    del self.csr_once[cmd]
            else:
                c = self.csr_once.pop(cmd)
            return bytes([c])
        if cmd < 128:
            return bytes([self._set(cmd, d)])
        if cmd == 155:
            return bytes([self.control])
        if cmd == 162:
            return self.status()
        if cmd == 165:
            return le(int(round(self.fwd)), 2)
        if cmd == 166:
            return le(int(round(self.reflected())), 2)
        if cmd == 193:
            return le(self.freq + self.readback_offset, 3)
        if cmd == 196:
            return le(self.duty, 2)
        return bytes([99])

    def _set(self, cmd: int, d: bytes) -> int:
        if cmd != 14 and self.control != 2:
            return 1
        if cmd == 1:
            self.out = False
            return 0
        if cmd == 2:
            if self.interlock_open:
                return 5
            self.out = True
            return 0
        if cmd == 3:
            if self.out:
                return 2
            self.reg_mode = d[0]
            return 0 if d[0] in (6, 7, 8) else 4
        if cmd == 8:
            v = u16(d)
            if v > 1000:
                return 4
            self.setpoint = v
            return 0
        if cmd == 14:
            if self.out and d[0] != self.control:
                self.out = False
            self.control = d[0]
            return 0
        if cmd == 27:
            if d[0] > 5:
                return 4
            self.pulsing = d[0]
            return 0
        if cmd == 93:
            v = u24(d)
            if not 1 <= v <= 100000:
                return 50
            self.freq = v
            return 0
        if cmd == 96:
            v = u16(d)
            if not 1 <= v <= 99:
                return 51
            self.duty = v
            return 0
        return 99

    def cmds_seen(self) -> List[int]:
        return [d[1] for _, d in self.received if len(d) > 1]

    def frames_seen(self) -> List[bytes]:
        return [d for _, d in self.received]

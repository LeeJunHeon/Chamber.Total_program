"""DC Pulse 시뮬레이터(챔버12) — EnerPulse EP5 RS-232 Type 4 프레임(v2.ch12.drivers.dcpulse_proto).

명령(쓰기 데이터 2 B MSB 먼저, 응답 ACK 06 / 거부 ERR 04)
  0x7B/0x7C/0x7D 마스터(3 = HOST) · 0x81 제어 모드(3 = Power) · 0x83 전력 raw(×10 W) · 0x80 출력(1 ON / 2 OFF)
  0x65 펄스 동기(0 Int) · 0x66 주파수 kHz(20~150) · 0x67 Off raw(9 = DC, 10~100) · 0x6F fault 지움 · 0x8C 아크 카운터 리셋(2)
읽기(응답 02 CMD DATA 03 CHK, DATA 2 B — 0x9A 만 6 B)
  0x90 상태(bit0 HV On) · 0x91 동작 정보 · 0x96 SAT · 0x99 ANT · 0x9A P/I/V · 0x9C 제어 모드(1 HOST/2 REMOTE/4 LOCAL)
  0x9E fault · 0xA5 동기 · 0xA6 주파수 · 0xA7 Off raw · 0xAE AE(0 고정) · 0xAF AF(1 고정) · 0xBB~0xBD 마스터
가정 모델(실제 장비 사실 아님): 출력 ON 이고 공용 챔버가 점화 조건이면 P = 설정, V = 400 V, I = P/V. 점화 안 되면 P = 0,
  V = 600 V, I = 0. 출력 ON 때 SAT/ANT 를 0 부터 다시 센다(v1 실측 2026-10-12~13), 출력 OFF 중 0x8C 는 효과 없음(같은 실측).
  아크는 ON 중 arc_rate_soft/hard(개/s)로 쌓인다. 응답 지연 = PROCESS_S + (요청 + 응답) 바이트 × 10 비트 / 9600(8N1 가정).
고장 주입(SimDevice 공통: no_response·delay·disconnect·garbage) + 장비 고유:
  ctrl_mode(REMOTE/LOCAL)·fault(0 이 아니면 0x6F 로 지워짐, fault_sticky 면 안 지워짐)·sync_ext·ignore_off_once(0x67 을 ACK 만 하고
  한 번 적용 안 함)·ignore_freq_once·on_ignored(0x80=1 을 ACK 하지만 켜지지 않음)·off_err_n(0x80=2 에 ERR n회)·
  i_override_a(전류 강제)·nak_reads{명령: 횟수}(그 읽기에 ERR)·write_err{명령: 횟수}(그 쓰기에 ERR).
"""
from __future__ import annotations

from typing import Dict, List, Optional

from v2.ch12.drivers import dcpulse_proto as P
from v2.ch12.sim.chamber import Chamber
from v2.common.simdev import SimDevice, sim_log

PROCESS_S = 0.01
BYTE_S = 10.0 / 9600.0
ON_V, OPEN_V = 400.0, 600.0


class DcPulseSim(SimDevice):
    def __init__(self, chamber: Optional[Chamber] = None) -> None:
        super().__init__("dcpulse", latency_s=0.0)
        self.chamber = chamber or Chamber()
        self.master = {P.MASTER_ONOFF: 1, P.MASTER_REFER: 1, P.MASTER_MODE: 1}
        self.ctrl_mode = 1
        self.fault = 0
        self.fault_sticky = False
        self.regulation = 1
        self.ref_raw = 0
        self.out = False
        self.sync = 0
        self.freq_khz = 40
        self.off_raw = P.OFF_RAW_DC
        self.sat = 0.0
        self.ant = 0.0
        self.arc_rate_soft = 0.0
        self.arc_rate_hard = 0.0
        self.ignore_off_once = False
        self.ignore_freq_once = False
        self.on_ignored = False
        self.off_err_n = 0
        self.i_override_a: Optional[float] = None
        self.nak_reads: Dict[int, int] = {}
        self.write_err: Dict[int, int] = {}
        self.t = self.chamber.t
        self.received = sim_log()

    # ───────── 시각 ─────────
    def _advance(self, now: float) -> None:
        self.chamber.advance_to(now)
        if now > self.t:
            if self.out:
                dt = now - self.t
                self.sat += self.arc_rate_soft * dt
                self.ant += self.arc_rate_hard * dt
            self.t = now
        self.chamber.dcp_w = self.power_w() if self.out else 0.0

    def set_w(self) -> float:
        return self.ref_raw * 10.0

    def power_w(self) -> float:
        if not self.out:
            return 0.0
        return self.set_w() if self.chamber.can_ignite() else 0.0

    def piv(self) -> tuple:
        if not self.out:
            return 0.0, 0.0, 0.0
        p = self.power_w()
        v = ON_V if p > 0 else OPEN_V
        i = p / v if p > 0 else 0.0
        if self.i_override_a is not None:
            i = self.i_override_a
        return p, i, v

    # ───────── 통신 ─────────
    def respond(self, data: bytes, now: float):
        self._advance(now)
        self.received.append((now, data))
        if self._active("no_response", now):
            return []
        out = self.handle(data, now)
        if out is None:
            return []
        if self._active("garbage", now) and len(out) > 1:
            out = out[:-1] + bytes([out[-1] ^ 0x5A])
        lat = PROCESS_S + (len(data) + len(out)) * BYTE_S + sum(f.extra_s for f in self._active("delay", now))
        return [(now + lat, out)]

    def handle(self, data: bytes, now: float) -> Optional[bytes]:
        if len(data) < 4 or data[0] != P.STX or data[-2] != P.ETX or P.chk(data[:-1]) != data[-1]:
            return bytes([P.ERR])
        cmd, d = data[1], data[2:-2]
        if cmd >= 0x90:
            if self.nak_reads.get(cmd, 0) > 0:
                self.nak_reads[cmd] -= 1
                return bytes([P.ERR])
            return self._read(cmd)
        if self.write_err.get(cmd, 0) > 0:
            self.write_err[cmd] -= 1
            return bytes([P.ERR])
        v = (d[0] << 8 | d[1]) if len(d) == 2 else (d[0] if d else 0)
        return bytes([P.ACK if self._write(cmd, v) else P.ERR])

    def _write(self, cmd: int, v: int) -> bool:
        if cmd in self.master:
            self.master[cmd] = v
            return True
        if self.ctrl_mode != 1:                  # Remote/Local: 시리얼은 모니터링만(매뉴얼 p.18 — 거부 방식은 가정)
            return False
        if cmd == P.REGULATION:
            self.regulation = v
            return v in (1, 2, 3)
        if cmd == P.REF_LEVEL:
            self.ref_raw = v
            return True
        if cmd == P.OUTPUT:
            if v == 1:
                if self.fault:
                    return False
                if not self.on_ignored:
                    if not self.out:
                        self.sat = self.ant = 0.0           # ON 때 다시 셈
                    self.out = True
                return True
            if v == 2:
                if self.off_err_n > 0:
                    self.off_err_n -= 1
                    return False
                self.out = False
                return True
            return False
        if cmd == P.PULSE_SYNC:
            self.sync = v
            return True
        if cmd == P.PULSE_FREQ:
            if not (P.FREQ_MIN_KHZ <= v <= P.FREQ_MAX_KHZ):
                return False
            if self.ignore_freq_once:
                self.ignore_freq_once = False
                return True
            self.freq_khz = v
            return True
        if cmd == P.OFF_TIME:
            if v != P.OFF_RAW_DC and not (10 <= v <= 100):
                return False
            if self.ignore_off_once:
                self.ignore_off_once = False
                return True
            self.off_raw = v
            return True
        if cmd == P.FAULT_RESET:
            if not self.fault_sticky:
                self.fault = 0
            return True
        if cmd == P.ARC_COUNT_RESET:
            if self.out and v == 2:
                self.sat = self.ant = 0.0
            return True
        return False

    def _read(self, cmd: int) -> bytes:
        u = (lambda x: bytes([(int(x) >> 8) & 0xFF, int(x) & 0xFF]))
        if cmd == P.R_PIV:
            p, i, v = self.piv()
            return P.frame(cmd, u(round(p / 10.0)) + u(round(i / 0.01)) + u(round(v)))
        val = {P.R_STATUS: 0x0001 if self.out else 0x0000, P.R_OPERATION: 0x0000,
               P.R_ARC_SOFT: int(self.sat) & 0xFFFF, P.R_ARC_HARD: int(self.ant) & 0xFFFF,
               P.R_CTRL_MODE: self.ctrl_mode, P.R_FAULT: self.fault, P.R_SYNC: self.sync,
               P.R_FREQ: self.freq_khz, P.R_OFF: self.off_raw, P.R_ARC_SOFT_RATE: 0, P.R_ARC_HARD_RATE: 1,
               P.R_MASTER_ONOFF: self.master[P.MASTER_ONOFF], P.R_MASTER_REFER: self.master[P.MASTER_REFER],
               P.R_MASTER_MODE: self.master[P.MASTER_MODE]}.get(cmd)
        if val is None:
            return bytes([P.ERR])
        return P.frame(cmd, u(val))

    # ───────── 시험용 ─────────
    def frames_seen(self) -> List[bytes]:
        return [d for _, d in self.received]

    def cmds_seen(self) -> List[int]:
        return [d[1] for _, d in self.received if len(d) > 1]

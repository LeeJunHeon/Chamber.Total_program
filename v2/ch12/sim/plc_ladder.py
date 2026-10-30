"""CH2 HEATER 래더(H0~H10) 근사 + 1차 지연 열 모델 — PLC 시뮬레이터(plc_sim)가 메모리(D·M)에 대고 돌린다.

출처: v1 tests/heater_ladder_sim.py(Ladder) 를 그대로 옮기고 시각 진행만 바꿨다(v1 시험은 1초 스캔, 여기는 0.1 s 단계 +
1초마다 램프 펄스). 주소는 래더 주소표 그대로(D = 레지스터 번호, M 코일 = 워드×16 + 비트):
  D110 PV · D112 SV · D113 SV 상한 · D114 워치독(소프트웨어가 바꿈) · D115 래더 사본 · D116 CUR_SV · D117 PID 오류 ·
  D118 MV 상한 · D119 램프 중간 목표 · D120 램프율 · D121 홀드백 · D122 유효 목표 · D123 이번 증가분 · D126 OT ·
  D127 감속 구간 · D128 감속 램프율 · D141 MV · D142~D145 Kp·Ti REAL · D146~D149 실제 Kp·Ti
  M640 RUN · M641 ITL · M642 FAULT · M643 RST · M644 OT · M645 TC_ERR · M646 WD_ERR · M649 PID_RUN · M652 DAC_EN ·
  M653 TC1_OK · M656 DA_OK · M657 WATER_OK · M658 VAC_OK(← M_V_2_인터락 코일 67)
워치독(H2): RUN 이고 D114 == D115(바뀌지 않음) 가 WD_TIMEOUT_S(30 s) 계속되면 WD_ERR → FAULT → RUN 리셋.
열 모델(가정값 — 실제 CH2 히터 값이 아니다, v1 시험 모델 그대로): dT/dt = (GAIN·f + plasma_heat − (T − 25)·(1 + gas_loss)) / TAU,
  f = (출력 − 400)/1200, 출력 = DAC_EN 이면 min(D141, D118). GAIN 2250 °C, TAU 900 s.
"""
from __future__ import annotations

import struct
from typing import List, Tuple

AMBIENT_C = 25.0
GAIN_C = 2250.0
TAU_S = 900.0
LADDER_MV_MIN = 400
DAC_SPAN = 1200.0
WD_TIMEOUT_S = 30.0
KP_DEFAULT, TI_DEFAULT = 3.0, 300.0
KP_BITS = (0x3DCCCCCD, 0x42480000)   # 0.1 ~ 50
TI_BITS = (0x41200000, 0x453B8000)   # 10 ~ 3000
VAC_ITL_COIL = 67                    # M_V_2_인터락(M00043) → M00412


def s16(v: int) -> int:
    v &= 0xFFFF
    return v - 65536 if v >= 32768 else v


def f2w(v: float) -> Tuple[int, int]:
    b = struct.unpack(">I", struct.pack(">f", float(v)))[0]
    return b & 0xFFFF, b >> 16


def _bits_to_float(b: int) -> float:
    return struct.unpack(">f", struct.pack(">I", b))[0]


class HeaterLadder:
    """D·M 리스트(시뮬레이터 메모리)를 직접 바꾼다."""

    def __init__(self, D: List[int], M: List[bool], gain: float = GAIN_C, tau: float = TAU_S) -> None:
        self.D, self.M = D, M
        self.T = AMBIENT_C
        self.gain, self.tau = gain, tau
        self.tc_offset = 0.0          # TC 가 실제보다 이만큼 다르게 읽힘
        self.gas_loss = 0.0
        self.plasma_heat = 0.0
        self.rst_self_reset = True    # False 면 래더가 RST 를 0 으로 되돌리지 않음
        self.water_ok = True          # WATER_2_SGN → M00411
        self.tc_fail = False          # 고장 주입: TC 단선(M653 = 0)
        self._wd_timer = 0.0
        self._integ = 0.0
        self._mv = 0.0
        self._pulse_acc = 0.0

    def _kp_ti(self) -> Tuple[float, float]:
        D = self.D
        kp, ti = KP_DEFAULT, TI_DEFAULT
        b = (D[143] << 16) | D[142]
        if KP_BITS[0] <= b <= KP_BITS[1]:
            kp = _bits_to_float(b)
        b = (D[145] << 16) | D[144]
        if TI_BITS[0] <= b <= TI_BITS[1]:
            ti = _bits_to_float(b)
        return kp, ti

    def _h0_defaults(self) -> None:
        D = self.D
        for addr, default in ((120, 2), (121, 100), (113, 5000), (126, 5500), (128, 10), (118, 520)):
            if D[addr] == 0:
                D[addr] = default
        D[113] = min(D[113], 9000)
        D[126] = min(D[126], 9500)
        if D[126] <= D[113]:
            D[126] = D[113] + 300
        D[118] = min(D[118], 1600)

    def _h1_h5(self, run: bool, dt: float) -> None:
        D, M = self.D, self.M
        if run and not M[653]:
            M[645] = True
        if M[653] and s16(D[110]) > D[126]:
            M[644] = True
        self._wd_timer = self._wd_timer + dt if (run and D[114] == D[115]) else 0.0
        if run and D[114] != D[115]:
            D[115] = D[114]
        if self._wd_timer >= WD_TIMEOUT_S:
            M[646] = True
        M[642] = M[644] or M[645] or M[646]
        M[641] = M[653] and M[656] and M[657] and M[658] and not M[642]
        if M[643]:
            M[644] = M[645] = M[646] = False
            if self.rst_self_reset:
                M[643] = False

    def _h6_ramp(self, run: bool, pulse: bool) -> None:
        D = self.D
        D[122] = min(D[112], D[113])
        if not run:
            D[119] = D[110]
        rem = s16(D[122]) - s16(D[119])
        lead = s16(D[119]) - s16(D[110])
        if run and rem < 0:
            D[119] = D[122]
        D[123] = D[120] if rem >= D[127] else D[128]
        if pulse and run and rem > 0 and lead < D[121]:
            D[119] = (s16(D[119]) + D[123]) & 0xFFFF
        if s16(D[119]) > s16(D[122]):
            D[119] = D[122]

    def _h7_pid(self, run: bool, dt: float) -> None:
        D, M = self.D, self.M
        if not (run and M[641]):
            return
        kp, ti = self._kp_ti()
        mv_max = D[118]
        e = s16(D[119]) - s16(D[110])
        if dt > 0:
            self._integ += e * dt
            u = kp * (e + self._integ / ti)
            if u > mv_max:
                if e > 0:
                    self._integ -= e * dt
                u = mv_max
            if u < LADDER_MV_MIN:
                if e < 0:
                    self._integ -= e * dt
                u = LADDER_MV_MIN
            self._mv = u
        D[141] = int(min(max(self._mv, LADDER_MV_MIN), mv_max))
        M[652] = M[649] = True

    def scan(self, dt: float) -> None:
        """래더 1스캔 + 열 모델 dt 초. 1초가 찰 때마다 램프 펄스(v1 시험의 1초 스캔과 같은 램프 속도)."""
        D, M = self.D, self.M
        pulse = False
        if dt > 0:
            self._pulse_acc += dt
            if self._pulse_acc >= 1.0 - 1e-9:
                self._pulse_acc -= 1.0
                pulse = True
        M[653] = not self.tc_fail
        M[656] = True
        M[657] = bool(self.water_ok)
        M[658] = bool(M[VAC_ITL_COIL])
        D[110] = int(round((self.T + self.tc_offset) * 10)) & 0xFFFF
        self._h0_defaults()
        kp, ti = self._kp_ti()
        D[146], D[147] = f2w(kp)
        D[148], D[149] = f2w(ti)
        run = M[640]
        self._h1_h5(run, dt)
        self._h6_ramp(run, pulse)
        self._h7_pid(run, dt)
        D[116] = D[119]
        D[117] = 0
        if (not run) or M[642] or (not M[641]):          # H9 출력 차단
            D[141] = 0
            M[652] = False
            if not run:
                self._integ = 0.0
                M[649] = False
        if M[642]:
            M[640] = False
        if not M[641]:                                   # H9: 인터락이 풀리면 RUN 리셋
            M[640] = False
        out = min(D[141], D[118]) if M[652] else 0
        f = max(0.0, (out - LADDER_MV_MIN) / DAC_SPAN)
        self.T += (self.gain * f + self.plasma_heat - (self.T - AMBIENT_C) * (1.0 + self.gas_loss)) / self.tau * dt

    @property
    def output(self) -> int:
        return min(self.D[141], self.D[118]) if self.M[652] else 0

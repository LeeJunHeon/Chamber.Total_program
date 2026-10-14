# -*- coding: utf-8 -*-
"""CH2 HEATER 래더(H0~H10) 근사 + 1차 지연 열 모델 — HeaterService 통합 시나리오 테스트용(가상 시계, 1초 스캔).

AsyncPLC 공개 메서드(is_connected / read_regs_block / read_coils_block / write_reg / write_coil)만 흉내낸다.
주소는 래더 주소표 그대로 쓴다(D = 레지스터 번호, M 코일 = 워드×16 + 비트) — 래더 화면과 한 줄씩 대조하기 위해서다.
쓰기마다 dt=0 스캔을 한 번 돌린다(실제 PLC 는 쓰기와 다음 읽기 사이에 수십 번 스캔한다).
열 모델: dT/dt = (GAIN·f + plasma_heat − (T − AMBIENT)·(1 + gas_loss)) / TAU, f = (출력 − 400) / 1200,
  출력 = DAC_EN 이면 min(D141, D118). gas_loss·plasma_heat 는 기본 0(진공) — 공정 조건 시나리오에서만 바꾼다.
  GAIN 2250, TAU 900초 → MV 520 정상상태 ≈ 250°C (300°C 목표는 도달 불가, 248°C 는 간신히 도달).
  실제 CH2 히터의 값이 아니다 — '상한이 모자라는 경우'를 재현하려고 고른 모델 값이다.
"""
from __future__ import annotations

import struct
from typing import Any, Callable, List, Optional, Tuple

from controller.heater_service import HeaterService
from lib.heater_config import HeaterConfig

AMBIENT_C = 25.0
GAIN_C = 2250.0
TAU_S = 900.0
LADDER_MV_MIN = 400
DAC_SPAN = 1200.0
WD_TIMEOUT_S = 30.0
KP_DEFAULT, TI_DEFAULT = 3.0, 300.0
KP_BITS = (0x3DCCCCCD, 0x42480000)   # 0.1 ~ 50
TI_BITS = (0x41200000, 0x453B8000)   # 10 ~ 3000


def _s16(v: int) -> int:
    """16비트 부호 있는 값."""
    v &= 0xFFFF
    return v - 65536 if v >= 32768 else v


def _f2w(v: float) -> Tuple[int, int]:
    """REAL → (하위 워드, 상위 워드)."""
    b = struct.unpack(">I", struct.pack(">f", float(v)))[0]
    return b & 0xFFFF, b >> 16


def _bits_to_float(b: int) -> float:
    """32비트 → REAL."""
    return struct.unpack(">f", struct.pack(">I", b))[0]


class Clock:
    """가상 단조 시계."""

    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


class Ladder:
    """HEATER 래더 근사 + 열 모델 + AsyncPLC 흉내."""

    def __init__(self, gain: float = GAIN_C, tau: float = TAU_S) -> None:
        self.D: List[int] = [0] * 200
        self.M: List[bool] = [False] * 700
        self.T = AMBIENT_C
        self.gain, self.tau = gain, tau
        self.tc_offset = 0.0          # TC 가 실제보다 이만큼 다르게 읽힘(셔터 개방 등 흉내)
        self.gas_loss = 0.0           # 공정 가스로 늘어나는 열손실 비율(0.25 = 25% 더 잃음)
        self.plasma_heat = 0.0        # 플라즈마가 더하는 열(°C 환산 정상상태 기여분)
        self.rst_self_reset = True    # False 면 래더가 RST 를 0 으로 되돌리지 않음
        self.connected = True
        self.reject_regs: set = set() # 이 주소 쓰기는 예외(PLC 쓰기 거부 흉내)
        self._wd_timer = 0.0
        self._integ = 0.0
        self._mv = 0.0

    def _kp_ti(self) -> Tuple[float, float]:
        """D142~D145 REAL 이 범위 안이면 그 값, 밖이면 래더 기본값."""
        kp, ti = KP_DEFAULT, TI_DEFAULT
        b = (self.D[143] << 16) | self.D[142]
        if KP_BITS[0] <= b <= KP_BITS[1]:
            kp = _bits_to_float(b)
        b = (self.D[145] << 16) | self.D[144]
        if TI_BITS[0] <= b <= TI_BITS[1]:
            ti = _bits_to_float(b)
        return kp, ti

    def _h0_defaults(self) -> None:
        """H0 기본값·클램프."""
        D = self.D
        for addr, default in ((120, 2), (121, 100), (113, 5000), (126, 5500), (128, 10), (118, 520)):
            if D[addr] == 0:
                D[addr] = default
        D[113] = min(D[113], 7000)
        D[126] = min(D[126], 7500)
        if D[126] <= D[113]:
            D[126] = D[113] + 300
        D[118] = min(D[118], 1600)

    def _h1_h5_latches(self, run: bool, dt: float) -> None:
        """H1 과온·TC 이상 래치, H2 워치독, H3/H4 FAULT·ITL, H5 리셋."""
        D, M = self.D, self.M
        if run and not M[653]:
            M[645] = True
        if M[653] and _s16(D[110]) > D[126]:
            M[644] = True
        self._wd_timer = self._wd_timer + dt if (run and D[114] == D[115]) else 0.0
        if run and D[114] != D[115]:
            D[115] = D[114]
        if self._wd_timer >= WD_TIMEOUT_S:
            M[646] = True
        M[642] = M[644] or M[645] or M[646]
        M[641] = M[653] and M[656] and not M[642]
        if M[643]:
            M[644] = M[645] = M[646] = False
            if self.rst_self_reset:
                M[643] = False

    def _h6_ramp(self, run: bool, pulse: bool) -> None:
        """H6 램프 중간 목표 D119 — 홀드백 D121, 감속 구간 D127/D128."""
        D = self.D
        D[122] = min(D[112], D[113])
        if not run:
            D[119] = D[110]
        rem = _s16(D[122]) - _s16(D[119])
        lead = _s16(D[119]) - _s16(D[110])
        if run and rem < 0:
            D[119] = D[122]
        D[123] = D[120] if rem >= D[127] else D[128]
        if pulse and run and rem > 0 and lead < D[121]:
            D[119] = (_s16(D[119]) + D[123]) & 0xFFFF
        if _s16(D[119]) > _s16(D[122]):
            D[119] = D[122]

    def _h7_pid(self, run: bool, dt: float) -> None:
        """H7 PID(정동작, 1초, MV_min 400, MV_max = D118, 포화 방향으로만 적분 정지)."""
        D, M = self.D, self.M
        if not (run and M[641]):
            return
        kp, ti = self._kp_ti()
        mv_max = D[118]
        e = _s16(D[119]) - _s16(D[110])
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

    def scan(self, dt: float = 1.0, pulse: bool = True) -> None:
        """래더 1스캔 + 열 모델 dt 초 진행. pulse=False 면 1초 램프 펄스 없음(쓰기 직후 스캔)."""
        D, M = self.D, self.M
        M[653] = True                                    # TC1_OK
        M[656] = True                                    # DA_OK
        D[110] = int(round((self.T + self.tc_offset) * 10)) & 0xFFFF
        self._h0_defaults()
        kp, ti = self._kp_ti()
        D[146], D[147] = _f2w(kp)
        D[148], D[149] = _f2w(ti)
        run = M[640]
        self._h1_h5_latches(run, dt)
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
        out = min(D[141], D[118]) if M[652] else 0
        f = max(0.0, (out - LADDER_MV_MIN) / DAC_SPAN)
        self.T += (self.gain * f + self.plasma_heat - (self.T - AMBIENT_C) * (1.0 + self.gas_loss)) / self.tau * dt

    # ── AsyncPLC 흉내 ──
    def is_connected(self) -> bool:
        """링크 상태."""
        return self.connected

    async def read_regs_block(self, start: int, count: int, *, priority: str = "high") -> List[int]:
        """FC3 블록 읽기."""
        if not self.connected:
            raise RuntimeError("E401")
        return [int(x) & 0xFFFF for x in self.D[start:start + count]]

    async def read_coils_block(self, start: int, count: int, *, priority: str = "high") -> List[bool]:
        """FC1 블록 읽기."""
        if not self.connected:
            raise RuntimeError("E401")
        return [bool(x) for x in self.M[start:start + count]]

    async def write_reg(self, addr: int, value: int) -> None:
        """FC6 쓰기 + 스캔 1회."""
        if not self.connected:
            raise RuntimeError("E401")
        if addr in self.reject_regs:
            raise RuntimeError("E403 write rejected")
        self.D[addr] = int(value) & 0xFFFF
        self.scan(0.0, pulse=False)

    async def write_coil(self, addr: int, on: bool) -> None:
        """FC5 쓰기 + 스캔 1회."""
        if not self.connected:
            raise RuntimeError("E401")
        self.M[addr] = bool(on)
        self.scan(0.0, pulse=False)


class FakeChat:
    """구글챗 카드 기록."""

    def __init__(self, clk: Clock) -> None:
        self.clk = clk
        self.cards: List[tuple] = []

    def _post_card(self, title: str, subtitle: str = "", status: str = "INFO",
                   fields: Optional[dict] = None, urgent: bool = False) -> None:
        """(시각, 제목, 부제, 상태) 로 남긴다."""
        self.cards.append((self.clk.t, title, subtitle, status))


def make(gain: float = GAIN_C, **cfg_over: Any):
    """(svc, plc, clk, logs, chat) — logs 는 [(시각, 문구)]. cfg_over 는 HeaterConfig 필드."""
    clk = Clock()
    plc = Ladder(gain=gain)
    chat = FakeChat(clk)
    logs: List[tuple] = []
    cfg = HeaterConfig(**{"enabled": True, "log_enabled": False, **cfg_over})
    svc = HeaterService(plc, cfg, log=lambda s, m: logs.append((clk.t, m)), chat=chat, clock=clk)
    return svc, plc, clk, logs, chat


async def run(svc: HeaterService, plc: Ladder, clk: Clock, secs: int, wd: bool = True,
              on_step: Optional[Callable[[HeaterService, Ladder], None]] = None) -> None:
    """1초마다: 래더 스캔 → 시계 +1 → 폴링 1회 → (RUN 이면 5초마다 워치독) → on_step(svc, plc)."""
    for _ in range(int(secs)):
        plc.scan(1.0)
        clk.t += 1.0
        await svc._poll_once()
        if wd and plc.M[640] and int(clk.t) % 5 == 0:
            await svc._kick_wd()
        if on_step is not None:
            on_step(svc, plc)

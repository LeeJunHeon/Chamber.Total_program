"""시뮬레이터 공용 챔버 모델(챔버12) — 챔버마다 하나(CH1·CH2). 장비 시뮬레이터들이 압력·밸브·가스·플라즈마·온도·두께를 함께 쓴다.

출처: 챔버K v2/chk/sim/chamber.py(de8e946) 의 1차 모델을 그대로 쓰고 챔버12 장비를 더했다.
시각은 advance_to(now) 로 0.1 s 단계씩(여러 시뮬레이터가 불러도 한 번만 진행). 값은 가정값이다 — 실제 장비 사실이 아니다.

넣는 쪽(시뮬레이터 → 모델)
- PLC 시뮬레이터: valves(mv·rv·fv·vent·turbo·rotary·ar·o2·n2·ms·gate·chuck 위치 …)·dc_set_w(PLC DAC)·rf_set_w(PLC DAC)·heater_mv
- MFC 시뮬레이터: gas_sccm[ar·o2·n2]·throttle(압력 제어 밸브 0~1)
- DC Pulse·RF Pulse 시뮬레이터: dcp_w·rfp_w(출력 중인 전력, 0 = 꺼짐)
읽는 쪽: IG(pressure)·MFC 압력 센서(pressure_mtorr)·PLC ADC(dc·rf 되읽기)·STM-100(thickness_A·rate_A_s)·히터(temp_c)

압력(Torr) — 챔버K 와 같은 1차 모델:
  Vent 열림 → 760, τ 20 s · MV·Turbo → BASE + 총 유량 × TORR_PER_SCCM / 유효 배기, τ 5 s · 러핑 → 0.05, τ 10 s · 그 밖 그대로
플라즈마: 가스 있음 + 압력 IGNITE_MIN~MAX mTorr 이면 출력(어느 전원이든) 이 있을 때 점화(ignited).
증착: 점화 + 메인 셔터(ms) 열림이면 rate = 총 전력(W) × RATE_A_PER_W_S Å/s 로 두께가 쌓인다(STM-100 이 읽음).
히터(CH2): temp_c 는 heater_mv(0~4000 count) 에 비례한 목표로 1차 지연(τ 120 s), 꺼지면 실온으로.
고장 주입: ignite_fail(점화 실패)·pressure_override(압력 고정)·xtal_fail(두께 센서 S 값 없음)
"""
from __future__ import annotations

import math
from typing import Dict, Optional

BASE_TORR = 5e-7
TORR_PER_SCCM = 1e-4
ROUGH_TORR = 0.05
THROTTLE_MIN = 0.02
ATM_TORR = 760.0
IGNITE_MIN_MTORR = 0.5
IGNITE_MAX_MTORR = 100.0
RATE_A_PER_W_S = 0.01          # 100 W → 1 Å/s
ROOM_C = 23.0
HEATER_C_PER_COUNT = 0.2       # MV 4000 → 823 °C 목표(가정)
HEATER_TAU_S = 120.0


class Chamber:
    def __init__(self, name: str = "ch1", start: float = 0.0) -> None:
        self.name = name
        self.t = start
        self.pressure = BASE_TORR
        self.pressure_override: Optional[float] = None
        self.valves: Dict[str, bool] = {"mv": True, "turbo": True, "rotary": True, "rv": False, "fv": True,
                                        "vent": False, "ms": False, "gate": False}
        self.gas_sccm: Dict[str, float] = {"ar": 0.0, "o2": 0.0, "n2": 0.0}
        self.throttle = 1.0
        self.ignite_fail = False
        self.xtal_fail = False
        self.dc_set_w = 0.0
        self.rf_set_w = 0.0
        self.dcp_w = 0.0
        self.rfp_w = 0.0
        self.ignited = False
        self.thickness_A = 0.0
        self.rate_A_s = 0.0
        self.heater_mv = 0
        self.heater_run = False
        self.temp_c = ROOM_C

    def set_valves(self, **v: bool) -> None:
        self.valves.update(v)

    def total_sccm(self) -> float:
        return sum(self.gas_sccm.values())

    def total_power_w(self) -> float:
        return max(0.0, self.dc_set_w) + max(0.0, self.rf_set_w) + max(0.0, self.dcp_w) + max(0.0, self.rfp_w)

    @property
    def pressure_mtorr(self) -> float:
        return self.pressure * 1000.0

    def can_ignite(self) -> bool:
        p = self.pressure_mtorr
        return not self.ignite_fail and self.total_sccm() > 0.0 and IGNITE_MIN_MTORR <= p <= IGNITE_MAX_MTORR

    def reset_thickness(self) -> None:
        self.thickness_A = 0.0

    def advance_to(self, now: float) -> None:
        while self.t < now - 1e-12:
            dt = min(0.1, now - self.t)
            self._step(dt)
            self.t += dt

    def _step(self, dt: float) -> None:
        if self.pressure_override is not None:
            self.pressure = self.pressure_override
        else:
            v = self.valves
            tgt = None
            if v.get("vent"):
                tgt, tau = ATM_TORR, 20.0
            elif v.get("mv") and v.get("turbo"):
                eff = THROTTLE_MIN + (1.0 - THROTTLE_MIN) * min(1.0, max(0.0, self.throttle))
                tgt, tau = BASE_TORR + self.total_sccm() * TORR_PER_SCCM / eff, 5.0
            elif v.get("rotary") and (v.get("rv") or v.get("fv")):
                tgt, tau = ROUGH_TORR, 10.0
            if tgt is not None:
                self.pressure += (tgt - self.pressure) * min(1.0, dt / tau)
        w = self.total_power_w()
        self.ignited = w > 0 and self.can_ignite()
        self.rate_A_s = w * RATE_A_PER_W_S if (self.ignited and self.valves.get("ms")) else 0.0
        self.thickness_A += self.rate_A_s * dt
        tgt_c = ROOM_C + (HEATER_C_PER_COUNT * self.heater_mv if self.heater_run else 0.0)
        self.temp_c += (tgt_c - self.temp_c) * min(1.0, dt / HEATER_TAU_S)

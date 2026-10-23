# controller/heater_process.py
# -*- coding: utf-8 -*-
"""공정이 쓰는 히터 대기 판정 — Qt·PLC 없음(상태 dict 만 본다). ChamberRuntime 이 히터 상태 리스너에서 tick 한다.

heater_wait_timeout_s : 승온(냉각) 대기 한도 — 예상 시간 + 여유와 기본값 중 큰 쪽, 상한 12시간.
HeaterSoakWaiter      : 램프 끝 + |PV−목표| ≤ tol 이 soak_sec 연속이면 도달(통과). hold_sec(레시피 heater_soak)이 있으면
                        도달 뒤 SOAK — 시간만 센다(온도는 래더 PID 가 계속 맞춘다, 범위를 벗어나도 다시 세지 않고 기록만).
                        히터 이상·꺼짐·PLC 무응답은 SOAK 중에도 실패. 시간 초과(E831)는 도달 전까지만 본다.
HeaterHoldWaiter      : 작업 압력·플라즈마 조건에서 유지 모드(dac)가 새로 잡힐 때까지.
                        잡히면 통과 / 포기·시간 초과면 실패 결과(처리는 호출부 — 강제 진입 또는 중단).
itl_missing_text      : 히터 인터락(ITL)이 꺼진 원인 문구 — 서비스·패널·런타임이 같이 쓴다.
heater_off_text       : 운전 중 RUN 이 꺼졌을 때의 사유 문구(인터락 해제면 빠진 조건을 붙인다).
tick 반환: None = 계속 / (True, 문구, None) = 통과 / (False, 문구, 오류코드) = 실패.
"""
from __future__ import annotations

import time
from typing import Callable, Optional, Tuple

from lib.heater_profile import ramp_minutes

WAIT_TIMEOUT_MARGIN_SEC = 1800.0   # 예상 시간에 더하는 여유(챔버K 와 같음)
WAIT_TIMEOUT_MAX_SEC = 43200.0     # 상한 12시간
COOL_RATE_C_PER_MIN = 1.0          # 목표가 현재보다 낮을 때 가정하는 자연 냉각 속도(보수적)
RAMP_DONE_TOL_C = 0.15             # D119 가 목표에 닿았다고 보는 허용치(0.1°C 눈금)
STALE_FAIL_SEC = 60.0              # 대기 중 PLC 응답 없음이 이만큼 이어지면 실패
PROGRESS_LOG_SEC = 60.0            # 진행 로그 간격

E_NOT_READY = "E830"   # 히터 사용 불가(설정·PLC 상태·대기 중 꺼짐)
E_TIMEOUT = "E831"     # 승온 시간 초과
E_FAULT = "E832"       # 공정 중 히터 이상
E_HOLD = "E833"        # 유지 모드 진입 실패(HEATER_HOLD_FAIL_ACTION=abort)

Result = Optional[Tuple[bool, str, Optional[str]]]

# 히터 인터락(ITL, M00401) 조건 — 래더 H4: TC1_OK · DA_OK · WATER_OK · VAC_OK · ¬FAULT
# (WATER_OK = WATER_2_SGN, VAC_OK = M_V_2_인터락 — 메인밸브 인터락에 공압·게이지A 진공·러핑·포라인·터보·벤트 닫힘이 들어 있다)
ITL_PARTS = (("tc_ok", "TC"), ("da_ok", "D/A"), ("water_ok", "냉각수"), ("vac_ok", "진공(메인밸브 인터락)"))


def itl_missing_text(st: dict) -> str:
    """히터 인터락이 꺼진 원인 문구 — 상태 dict 의 조건 비트 중 꺼진 것('TC, 진공(메인밸브 인터락)' 형식)."""
    names = [label for key, label in ITL_PARTS if not st.get(key)]
    if st.get("fault"):
        names.append("FAULT")
    return ", ".join(names) or "원인 미상"


def heater_off_text(st: dict) -> str:
    """운전 중 RUN 이 꺼졌을 때의 사유 — 인터락이 꺼져 있으면 래더가 끈 것(H9: ¬ITL → RUN 리셋)."""
    if st.get("ok") is not None and not st.get("stale") and not st.get("fault") and not st.get("itl"):
        return f"히터 인터락 해제({itl_missing_text(st)}) — PLC 가 히터를 끔"
    return "히터가 꺼짐(공정 밖에서 OFF 또는 PLC 가 RUN 해제)"


def _fmt(v) -> str:
    return "--.-" if v is None else f"{float(v):.1f}"


def _dur(sec: float) -> str:
    """기간 문구 — 2분 미만은 초('60초'), 그 이상은 분('60분', '7.5분')."""
    s = max(0.0, float(sec))
    if s < 120.0:
        return f"{s:.0f}초"
    return f"{s / 60:.1f}".rstrip("0").rstrip(".") + "분"


def heater_wait_timeout_s(pv: Optional[float], target: float, rate: float, *, base_timeout_s: float,
                          approach_zone: float, approach_min_rate: float, soak_sec: float) -> Tuple[float, str]:
    """(한도 초, 근거 문구). 올릴 때는 감속 접근을 넣은 예상 램프 시간, 내릴 때는 자연 냉각 1°C/min 가정."""
    base = float(base_timeout_s)
    if pv is None:
        return min(base, WAIT_TIMEOUT_MAX_SEC), "현재 온도 미상 → 기본값 HEATER_WAIT_TIMEOUT_SEC"
    span = float(target) - float(pv)
    if span >= 0:
        need = ramp_minutes(span, float(rate), approach_zone, approach_min_rate) * 60.0 + float(soak_sec)
        basis = f"예상 승온 {need / 60:.0f}분({float(rate):g}°C/min, 감속 접근 포함)"
    else:
        need = (-span) / COOL_RATE_C_PER_MIN * 60.0 + float(soak_sec)
        basis = f"예상 냉각 {need / 60:.0f}분(자연 냉각 {COOL_RATE_C_PER_MIN:g}°C/min 가정)"
    t = min(max(base, need + WAIT_TIMEOUT_MARGIN_SEC), WAIT_TIMEOUT_MAX_SEC)
    if t >= WAIT_TIMEOUT_MAX_SEC:
        return t, f"{basis} → 상한 {WAIT_TIMEOUT_MAX_SEC / 3600:.0f}시간"
    if need + WAIT_TIMEOUT_MARGIN_SEC >= base:
        return t, f"{basis} + 여유 {WAIT_TIMEOUT_MARGIN_SEC / 60:.0f}분"
    return t, "기본값 HEATER_WAIT_TIMEOUT_SEC"


class _Base:
    def __init__(self, timeout_s: float, clock: Callable[[], float], log: Callable[[str], None]):
        self._now = clock
        self._log = log
        self.t0 = clock()
        self.timeout_s = float(timeout_s)
        self._stale_since = 0.0
        self._last_log = self.t0

    def _common(self, st: dict) -> Result:
        """이상·응답 없음·꺼짐 공통 판정. stale 이면 (실패가 아닐 때) None 을 돌려주고 호출부도 판정을 쉰다."""
        now = self._now()
        if st.get("fault"):
            return False, "히터 이상 발생", E_FAULT
        if st.get("stale"):
            if not self._stale_since:
                self._stale_since = now
            elif now - self._stale_since >= STALE_FAIL_SEC:
                return False, f"히터 PLC 응답 없음 {now - self._stale_since:.0f}초", E_NOT_READY
            return None
        self._stale_since = 0.0
        if not st.get("run"):
            return False, heater_off_text(st), E_NOT_READY
        return None


class HeaterSoakWaiter(_Base):
    def __init__(self, target: float, *, tol: float, soak_sec: float, timeout_s: float, hold_sec: float = 0.0,
                 clock: Callable[[], float] = time.monotonic, log: Callable[[str], None] = lambda m: None):
        super().__init__(timeout_s, clock, log)
        self.target = float(target)
        self.tol = float(tol)
        self.soak_sec = float(soak_sec)
        self._band_since = 0.0
        self.hold_sec = max(0.0, float(hold_sec))   # 도달 뒤 SOAK(초, 레시피 heater_soak) — 0 = 없음(예전 그대로)
        self.hold_t0 = 0.0                          # SOAK 시작 시각(0 = 아직 도달 전)
        self.pv_min: Optional[float] = None         # SOAK 중 PV 최저·최고
        self.pv_max: Optional[float] = None
        self.out_sec = 0.0                          # SOAK 중 목표 ±tol 밖에 있던 시간(초)
        self._out_since = 0.0

    def pv_range_text(self) -> str:
        """SOAK 중 PV 범위 문구('399.8~400.4°C' + 범위 밖에 있었으면 그 시간)."""
        if self.pv_min is None:
            return "PV 기록 없음"
        txt = f"{self.pv_min:.1f}~{self.pv_max:.1f}°C"
        if self.out_sec >= 1.0:
            txt += f", ±{self.tol:g}°C 밖 {_dur(self.out_sec)}"
        return txt

    def _tick_hold(self, st: dict, now: float) -> Result:
        """도달 뒤 SOAK — 시간만 센다. 범위를 벗어나고 돌아온 것은 기록만 한다(다시 세지 않음)."""
        pv = st.get("pv")
        if pv is not None:
            pv = float(pv)
            self.pv_min = pv if self.pv_min is None else min(self.pv_min, pv)
            self.pv_max = pv if self.pv_max is None else max(self.pv_max, pv)
            out = abs(pv - self.target) > self.tol
            if out and not self._out_since:
                self._out_since = now
                self._log(f"히터 SOAK 중 PV {pv:.1f}°C — 목표 ±{self.tol:g}°C 밖(SOAK 시간은 계속 셈, 온도는 PID 가 맞춤)")
            elif not out and self._out_since:
                self.out_sec += now - self._out_since
                self._log(f"히터 PV 범위 복귀 — {pv:.1f}°C ({_dur(now - self._out_since)} 벗어났었음)")
                self._out_since = 0.0
        el = now - self.hold_t0
        if el >= self.hold_sec:
            if self._out_since:
                self.out_sec += now - self._out_since
                self._out_since = 0.0
            return True, (f"히터 준비 완료 — SOAK {_dur(self.hold_sec)} 끝, PV {_fmt(pv)}°C "
                          f"(SOAK 중 {self.pv_range_text()}), 대기 {(now - self.t0) / 60:.1f}분"), None
        if now - self._last_log >= PROGRESS_LOG_SEC:
            self._last_log = now
            self._log(f"히터 SOAK — {_dur(el)} / {_dur(self.hold_sec)}, 남은 {_dur(self.hold_sec - el)} "
                      f"(PV {_fmt(pv)} / 목표 {self.target:.1f}°C)")
        return None

    def tick(self, st: dict, ramp_active: bool) -> Result:
        r = self._common(st)
        if r is not None or st.get("stale"):
            return r
        now = self._now()
        if self.hold_t0:
            return self._tick_hold(st, now)
        pv = st.get("pv"); sv = st.get("sv"); svr = st.get("sv_ramp")
        if now - self._last_log >= PROGRESS_LOG_SEC:
            self._last_log = now
            self._log(f"히터 승온 대기 — PV {_fmt(pv)} / 목표 {self.target:.1f}°C, {(now - self.t0) / 60:.0f}분 경과")
        ramp_done = (not ramp_active and sv is not None and svr is not None
                     and abs(float(sv) - self.target) <= 0.05 and abs(float(svr) - self.target) <= RAMP_DONE_TOL_C)
        if ramp_done and pv is not None and abs(float(pv) - self.target) <= self.tol:
            if not self._band_since:
                self._band_since = now
                self._log(f"히터 목표 도달 — {self.soak_sec:.0f}초 안정 확인 중 (PV {float(pv):.1f}°C)")
            elif now - self._band_since >= self.soak_sec:
                if self.hold_sec <= 0:
                    return True, f"히터 준비 완료 — PV {float(pv):.1f}°C, 대기 {(now - self.t0) / 60:.1f}분", None
                self.hold_t0 = now
                self._last_log = now
                self._log(f"히터 도달 확인 — PV {float(pv):.1f}°C, {(now - self.t0) / 60:.1f}분. SOAK {_dur(self.hold_sec)} 시작"
                          f"(시간만 셈 — 온도가 흔들려도 다시 세지 않음)")
                return None
        else:
            self._band_since = 0.0
        if now - self.t0 >= self.timeout_s:
            return (False, f"히터 승온 시간 초과({self.timeout_s / 60:.0f}분) — PV {_fmt(pv)} / 목표 {self.target:.1f}°C",
                    E_TIMEOUT)
        return None


class HeaterHoldWaiter(_Base):
    """유지 모드가 '이 대기를 시작한 뒤' 새로 잡히거나 포기될 때까지(포기는 give_up_seq 증가로 판단)."""

    def __init__(self, *, timeout_s: float, give_up_seq0: int = 0, clock: Callable[[], float] = time.monotonic,
                 log: Callable[[str], None] = lambda m: None):
        super().__init__(timeout_s, clock, log)
        self._seq0 = int(give_up_seq0)
        self.fail_kind = ""          # 실패 때 "give_up"(출력 상한에 붙어 포기) / "timeout"(시간 안에 못 잡음)

    def tick(self, st: dict, hold: dict) -> Result:
        r = self._common(st)
        if r is not None or st.get("stale"):
            return r
        now = self._now()
        if hold.get("holding"):
            return True, f"유지 모드 진입 — DAC 상한 {hold.get('value')} 고정({now - self.t0:.0f}초)", None
        if int(hold.get("give_up_seq") or 0) > self._seq0:
            self.fail_kind = "give_up"
            return False, f"유지 모드 포기 — {hold.get('give_up_text') or hold.get('gave_up') or ''}", E_HOLD
        if now - self.t0 >= self.timeout_s:
            self.fail_kind = "timeout"
            return False, f"유지 모드 진입 시간 초과({self.timeout_s:.0f}초, 상태 {hold.get('state')})", E_HOLD
        return None

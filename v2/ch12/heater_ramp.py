"""CH2 히터 파이썬 램프 — 목표 직전 감속 접근. v1 controller/heater_ramp.py(RampProfiler) 그대로(asyncio 대신 동기 호출).

[왜 파이썬이 램프를 만드나] 래더 램프는 D120 카운트/초 단위라 최소 1카운트/초 = 6°C/min 이다. 그보다 느린 램프와 도착 감속은
  파이썬이 SV(D112)를 매 폴링 조금씩 올려서 만든다. 래더는 항상 파이썬보다 빠르게 열어 두고(D120) D119(램프 중간 목표)가
  그 SV 를 따라가게 한다.
[앞섬 한계(LEAD)와 홀드백] 새 SV 는 항상 D119 + APPROACH_LEAD 이하 — PV 가 처져 래더가 홀드백으로 멈추면 파이썬 SV 도 멈춘다.
[거리 기준] 속도를 '남은 거리'의 함수로 정하므로 멈췄다 재개해도 보정이 필요 없다.

자체 타이머가 없다. 엔진(heater_engine)이 성공한 폴링마다 tick() 을 부른다(PLC worker 안 — 쓰기 함수는 그 자리에서 바로 쓴다).
쓰기 함수(write_sv(°C), write_ramp_counts(n))는 실패하면 예외를 던진다(v1 의 await 실패와 같음).
"""
from __future__ import annotations

import math
import time
from typing import Callable, Optional

from v2.ch12.heater_profile import HeaterCfg, approach_rate, ramp_minutes


class RampProfiler:
    """SV(D112)를 밀어 올려 감속 접근 램프를 만든다."""

    def __init__(self, get_status: Callable[[], dict], write_sv: Callable[[float], None],
                 write_ramp_counts: Callable[[int], None], log: Callable[[str], None],
                 cfg: HeaterCfg, clock: Callable[[], float] = time.monotonic) -> None:
        self._status = get_status
        self._write_sv = write_sv
        self._write_ramp = write_ramp_counts
        self._log = log
        self._cfg = cfg
        self._now = clock
        self._active = False
        self._target: Optional[float] = None
        self._rate = 0.0
        self._sv_cmd: Optional[float] = None
        self._t_last = 0.0
        self._label = ""

    @property
    def cfg(self) -> HeaterCfg:
        return self._cfg

    @cfg.setter
    def cfg(self, c: HeaterCfg) -> None:
        self._cfg = c

    # ---------- 조회 ----------
    def is_active(self) -> bool:
        return bool(self._active)

    def target(self) -> Optional[float]:
        """활성 램프의 최종 목표 [°C]. 활성이 아니면 None."""
        return self._target if self._active else None

    def rate(self) -> float:
        return float(self._rate)

    def sv_cmd(self) -> Optional[float]:
        return self._sv_cmd

    def remaining_minutes(self) -> Optional[float]:
        if not self._active or self._target is None:
            return None
        try:
            svr = (self._status() or {}).get("sv_ramp")
        except Exception:
            svr = None
        base = float(svr) if svr is not None else float(self._sv_cmd or 0.0)
        return ramp_minutes(max(0.0, float(self._target) - base), self._rate,
                            self._cfg.approach_zone, self._cfg.approach_min_rate)

    # ---------- 제어 ----------
    def start(self, target: float, rate: float, label: str = "") -> bool:
        """감속 접근 램프 시작. 못 쓰는 상황이면 False(호출부가 SV 를 직접 쓴다)."""
        try:
            st = self._status() or {}
        except Exception:
            st = {}
        svr = st.get("sv_ramp")
        pv = st.get("pv")
        if st.get("run") and svr is not None:
            base = svr
        elif pv is not None:
            base = pv
        else:
            base = svr
        if base is None:
            self._log("램프: 현재 온도를 읽을 수 없어 감속 접근을 쓸 수 없습니다")
            return False
        base = float(base)
        target = float(target)
        rate = float(rate)
        if target <= base + 0.05 or rate <= 0:
            return False
        counts = max(2, int(math.ceil(rate / 6.0)) + 1)
        try:
            self._write_ramp(counts)
            # ★ RUN 을 켜기 전에 SV 를 출발점으로 — 예전 목표가 남아 있으면 래더가 첫 1초를 자기 속도로 달려 버린다.
            self._write_sv(round(base, 1))
        except Exception as e:
            self._log(f"램프 시작 쓰기 실패 → 직접 목표 기록으로 대체: {e!r}")
            return False
        self._sv_cmd = base
        self._target = target
        self._rate = rate
        self._label = label or ""
        self._t_last = self._now()
        self._active = True
        lb = (" " + self._label) if self._label else ""
        self._log(
            f"램프{lb}: {base:.1f}→{target:.1f}°C · {rate:g}°C/min · 접근 {self._cfg.approach_zone:g}°C→"
            f"{self._cfg.approach_min_rate:g}°C/min · 예상 "
            f"{ramp_minutes(target - base, rate, self._cfg.approach_zone, self._cfg.approach_min_rate):.1f}분"
            f" (래더 D120 은 {counts * 6}°C/min 로 열어 둠)")
        return True

    def tick(self, dt_sec: Optional[float] = None) -> None:
        """남은 거리로 속도를 정하고 그만큼 SV 를 올린다."""
        if not self._active:
            return
        try:
            st = self._status() or {}
        except Exception:
            return
        svr = st.get("sv_ramp")
        if svr is None:
            return
        svr = float(svr)
        now = self._now()
        dt = (now - self._t_last) if dt_sec is None else float(dt_sec)
        dt = min(max(dt, 0.0), 5.0)
        self._t_last = now
        target = float(self._target)
        dist = max(0.0, target - svr)
        r_now = approach_rate(dist, self._rate, self._cfg.approach_zone, self._cfg.approach_min_rate)
        base = max(float(self._sv_cmd), svr - 0.05)
        new = min(target, base + r_now * dt / 60.0, svr + float(self._cfg.approach_lead))
        new = max(new, float(self._sv_cmd))
        if round(new, 1) != round(float(self._sv_cmd), 1):
            try:
                self._write_sv(round(new, 1))
            except Exception as e:
                self._log(f"램프 SV 쓰기 실패(다음 폴링에 재시도): {e!r}")
                return
        self._sv_cmd = new
        if self._sv_cmd >= target - 1e-6 and svr >= target - 0.15:
            self._active = False
            self._log(f"램프 완료 {target:.1f}°C")

    def stop(self, restore_rate: bool = True) -> None:
        """램프를 그만둔다. SV 는 건드리지 않는다. restore_rate 면 D120 을 설정값으로 되돌린다."""
        was = self._active
        self._active = False
        self._target = None
        if restore_rate and was:
            try:
                self._write_ramp(self._cfg.rate_counts(self._cfg.ramp_rate))
            except Exception as e:
                self._log(f"래더 램프 속도 원복 실패: {e!r}")

    def adopt(self, target: float, rate: float, sv_cmd: float, label: str = "") -> None:
        """재시작 이어가기 — 저널의 램프(목표·속도·마지막 SV)를 쓰기 없이 다시 잡는다(D120 은 PLC 에 그대로 남아 있다)."""
        self._target, self._rate, self._sv_cmd = float(target), float(rate), float(sv_cmd)
        self._label = label or ""
        self._t_last = self._now()
        self._active = True

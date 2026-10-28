# controller/thickness_process.py
# -*- coding: utf-8 -*-
"""두께 모니터 공정 연동 — Qt·asyncio 비의존 순수 판정. HeaterSoakWaiter 처럼 상태 dict 를 받아 tick() 한다.

모드 판정(thickness_mode_of)은 이 한 곳에 둔다 — 런타임(시작·시퀀스)과 ETA 계산이 같은 규칙을 쓴다.

ThicknessEndJudge.tick(status, now) → None | ThicknessEvent
  done     : 목표 도달(정상 종료). 복귀 순간 이미 목표 이상이어도 done.
  fallback : '시간 대체' 로 전환 — remaining_s 만큼 더 증착하고 끝낸다(타이머는 공정 단계가 가진다).
             permanent=True(XTAL FAIL·공정 중 리셋·판정 예외)면 두께 판정으로 돌아오지 않는다.
  resume   : 통신이 돌아와(리셋 없음) 두께 판정으로 복귀 — 공정 단계가 시간 대체 타이머를 지운다.
판정 규칙(STM-100 이 초당 4회 두께를 계산해 주므로 그 값으로 끊는다 — Evaporator 실제 공정과 같은 방식):
  - 기준점(유효두께 = S − 기준점)은 영점(B)이 반영된 것을 확인하고 잡는다. 영점 성공(zero_ok)이면 영점 뒤 새 값이
    zero_confirm_nm + 영점 뒤 증착 분량(max(레시피·장비 증착률) × 경과 × 1.5) 이하일 때(음수 = 셔터 열 충격 하락도 포함) 영점 반영 확인
    → 기준점 0(영점 시각). zero_confirm_s 안에 확인 안 되면(영점이 안 먹었거나 늦음) 그때 값을 기준점으로 잡고 경고.
    영점 실패면 영점 뒤 첫 새 값을 기준점으로. 기준점 전 값은 판정에 쓰지 않는다(늦게 반영된 영점 전 두께로 끝나지 않게).
    기준점 뒤 하락은 재설정하지 않는다 — 열 충격 하락·회복은 장비 두께 그대로(유효두께가 잠깐 음수였다 돌아옴).
  - status.seq 가 바뀐 값만 판정한다(같은 값 중복 금지).
  - 종료: 유효두께 ≥ 목표 인 새 값이 done_consecutive 번 연속. 하나라도 목표 미만이면 다시 센다.
    (목표 근처 한 표본 스파이크로 일찍 끝나지 않고, 잡음 σ 0.3 nm 까지 초과는 몇 표본 이내 — tests/test_thickness_judge_noise.py)
  - stale_s 동안 새 값이 없으면 시간 대체. 남은 시간 = (목표 − 마지막 유효두께) ÷ 사용 증착률
    (측정 measured_rate_min_s 이상이면 실측 평균, 아니면 레시피 dep_rate). 기준점을 잡기 전에 끊겼다가 돌아오면
    영점이 성공했으면 기준점 0(영점 시각), 아니면 기준을 알 수 없으므로 영구 시간 대체.
  - 기록만(판정에 영향 없음): 한 번에 크게 뛴 값(증가량 > max(jump_min_nm, 레시피 dep_rate × jump_rate_k × Δt)) 횟수,
    증착률 급등, 두께 음수, 증착률 0 지속.
  - 판정 코드 예외는 삼키지 않고 곧바로 영구 시간 대체(traceback 은 event.detail).
"""
from __future__ import annotations

import traceback
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple

from lib.thickness_config import ThicknessJudgeParams

END_THICKNESS = "Thickness"      # 종료 방식(공정 기록·카드 표기)
END_FALLBACK = "시간대체"
END_LIMIT = "상한"
END_TIME = "Time"
_EPS_NM = 1e-6                   # Å→nm 환산(×0.1)·기준점 뺄셈의 부동소수점 오차 — 100.0 nm 가 99.9999999 로 읽혀 한 번 늦게 끝나지 않게


def _f(v) -> float:
    try:
        return float(str(v).strip()) if v is not None and str(v).strip() else 0.0
    except Exception:
        return 0.0


def thickness_mode_of(thickness: Any, supported: bool) -> bool:
    """Thickness 모드 판정 — thickness > 0 이고 그 채널이 두께 모니터를 쓴다(SUPPORTS_THICKNESS=true + 서비스 붙음).
    아니면 Time 모드(지금 그대로: thickness 가 있으면 process_time 역산 → 시간 종료).
    process_time 칸이 비었는지는 보지 않는다(UI 가 thickness 입력 때 processTime 을 채운다)."""
    return bool(supported) and _f(thickness) > 0.0


def expected_seconds(thickness_nm: float, dep_rate_nm_s: float) -> float:
    """예상시간 = thickness ÷ dep_rate (ETA·상한·대체 시간 계산용)."""
    return float(thickness_nm) / float(dep_rate_nm_s)


def max_time_s(expected_s: float, p: ThicknessJudgeParams) -> float:
    """최대 시간 상한 = max(예상 × ratio, 예상 + margin)."""
    e = float(expected_s)
    return max(e * float(p.max_time_ratio), e + float(p.max_time_margin_s))


def fmt_mmss(sec: Optional[float]) -> str:
    if sec is None or sec != sec or sec < 0:
        return "--:--"
    s = int(round(sec))
    return f"{s // 60:02d}:{s % 60:02d}"


@dataclass
class ThicknessEvent:
    kind: str                            # "done" | "fallback" | "resume"
    reason: str = ""
    remaining_s: Optional[float] = None  # fallback: 시간 대체 남은 시간
    permanent: bool = False              # fallback: 두께 판정으로 돌아오지 않음
    eff_nm: Optional[float] = None
    detail: str = ""                     # 판정 예외 traceback


class ThicknessEndJudge:
    def __init__(self, target_nm: float, dep_rate_nm_s: float, p: ThicknessJudgeParams, *,
                 start_t: float, seq0: int = 0, reset0: int = 0, zero_ok: bool = True,
                 log: Optional[Callable[[str], None]] = None):
        self.target = float(target_nm)
        self.dep_rate = float(dep_rate_nm_s)
        self.p = p
        self.t0 = float(start_t)
        self._seq_last = seq0
        self._reset0 = int(reset0 or 0)
        self.zero_ok = bool(zero_ok)         # 공정 시작 영점(B) 성공 — 기준점 없는 복귀 때 기준 0 을 쓸 수 있는가
        self._log_cb = log
        self.events: List[str] = []          # 로그 문구(테스트·기록)

        self.mode = "thickness"              # "thickness" | "fallback"
        self.permanent = False
        self.fallback_reason = ""            # 마지막 시간 대체 전환 사유
        self.fallback_count = 0
        self.base: Optional[float] = None
        self.base_t: Optional[float] = None
        self.prev_s: Optional[float] = None
        self.prev_t: Optional[float] = None
        self.last_new_t = float(start_t)
        self.done_n = 0
        self.jump_count = 0                  # 한 번에 크게 뛴 값 횟수(기록만)
        self.eff = 0.0                       # 마지막 유효두께
        self.eff_t: Optional[float] = None   # 그 값을 받은 시각
        self._last_inc_t: Optional[float] = None
        self._zero_warned = False
        self._spike_on = False
        self._neg_warned = False

    # ── 공개 ──
    def tick(self, st: dict, now: float) -> Optional[ThicknessEvent]:
        try:
            return self._tick(st or {}, float(now))
        except Exception as e:
            tb = traceback.format_exc()
            return self._to_fallback(now, f"판정 코드 예외({e!r})", permanent=True, detail=tb)

    def use_rate(self) -> Tuple[float, str]:
        """시간 대체 증착률 — (nm/s, '실측'|'레시피')."""
        if self.eff_t is not None and self.base_t is not None:
            span = self.eff_t - self.base_t
            if span >= float(self.p.measured_rate_min_s) and self.eff > 0:
                return self.eff / span, "실측"
        return self.dep_rate, "레시피"

    def fallback_remaining_s(self) -> float:
        rate, _src = self.use_rate()
        left = max(0.0, self.target - self.eff)
        return left / rate if rate > 0 else left / max(self.dep_rate, 1e-9)

    def avg_rate(self) -> Optional[float]:
        """평균 증착률(유효두께 ÷ 기준점 뒤 경과) — 기록용."""
        if self.eff_t is None or self.base_t is None or self.eff_t <= self.base_t:
            return None
        return self.eff / (self.eff_t - self.base_t)

    def view(self, st: dict, now: float) -> Tuple[str, float]:
        """상태 문구·진행률(%) — "두께 x.x / y.y nm (nn%) · 증착률 r nm/s · 예상 남은 시간 mm:ss"."""
        eff = self.eff
        pct = max(0.0, eff / self.target * 100.0) if self.target > 0 else 0.0
        if self.mode == "fallback":
            rate, _src = self.use_rate()
        else:
            r = st.get("rate_nm_s")
            rate = float(r) if isinstance(r, (int, float)) and r > 0 else (self.avg_rate() or self.dep_rate)
        remain = (self.target - eff) / rate if rate > 0 else None
        head = "[시간 대체] " if self.mode == "fallback" else ""
        return (f"{head}두께 {eff:.1f} / {self.target:.1f} nm ({pct:.0f}%) · 증착률 {rate:.3f} nm/s · "
                f"예상 남은 시간 {fmt_mmss(max(0.0, remain) if remain is not None else None)}"), pct

    # ── 내부 ──
    def _log(self, msg: str) -> None:
        self.events.append(msg)
        if self._log_cb:
            try:
                self._log_cb(msg)
            except Exception:
                pass

    def _set_eff(self, eff: float, now: float) -> None:
        self.eff = eff
        self.eff_t = now

    def _to_fallback(self, now, reason: str, *, permanent: bool, detail: str = "") -> Optional[ThicknessEvent]:
        if self.mode == "fallback":
            if permanent and not self.permanent:
                self.permanent = True
                self.fallback_reason = reason
                self._log(f"시간 대체 유지 — 두께 판정으로 돌아가지 않음: {reason}")
            return None
        self.mode = "fallback"
        self.permanent = bool(permanent)
        self.fallback_reason = reason
        self.fallback_count += 1
        self.done_n = 0
        try:
            rem = self.fallback_remaining_s()
            rate, src = self.use_rate()
            how = f"(목표 {self.target:.1f} − 유효두께 {self.eff:.1f} nm) ÷ {src} {rate:.4f} nm/s"
        except Exception:
            rem, how = self.target / max(self.dep_rate, 1e-9), "레시피 예상시간"
        self._log(f"⚠ 시간 대체로 전환 — {reason}. 남은 시간 {rem:.0f}초 = {how}"
                  + ("" if permanent else " · 통신이 돌아오면 두께 판정으로 복귀"))
        return ThicknessEvent("fallback", reason=reason, remaining_s=rem, permanent=bool(permanent),
                              eff_nm=self.eff, detail=detail)

    def _tick(self, st: dict, now: float) -> Optional[ThicknessEvent]:
        p = self.p
        if self.permanent:
            return None
        rs = int(st.get("reset_seen") or 0)
        if rs > self._reset0:
            return self._to_fallback(now, f"공정 중 두께 모니터 전원 리셋({rs - self._reset0}회)", permanent=True)
        if st.get("crystal_ok") is False and not st.get("stale"):
            return self._to_fallback(now, "XTAL FAIL(크리스탈 이상)", permanent=True)

        seq, s = st.get("seq"), st.get("thickness_nm")
        if seq is None or seq == self._seq_last or s is None:
            if self.mode == "thickness" and (now - self.last_new_t) > float(p.stale_s):
                return self._to_fallback(now, f"새 두께 값 없음 {now - self.last_new_t:.0f}초", permanent=False)
            return None
        self._seq_last = seq
        self.last_new_t = now
        s = float(s)

        if self.mode == "fallback":          # 통신 복귀(리셋 없음) — 장비는 끊긴 동안에도 두께를 누적했다
            if self.base is None:
                # 영점 뒤 첫 값 전에 끊겼다 — 지금 값을 기준점으로 잡으면 끊긴 동안 쌓인 두께를 다시 센다
                if not self.zero_ok:
                    self.permanent = True
                    self.fallback_reason = "영점 뒤 기준점을 잡지 못함(영점 실패 + 첫 값 전 통신 끊김)"
                    self._log(f"시간 대체 유지 — 두께 판정으로 돌아가지 않음: {self.fallback_reason}")
                    return None
                self.base, self.base_t = 0.0, self.t0
                self._log(f"기준점 없이 복귀 — 영점 성공이므로 기준점 0 nm(영점 시각), 읽은 값 {s:.2f} nm 를 유효두께로")
            self.mode = "thickness"
            eff = s - self.base
            self._set_eff(eff, now)
            self.prev_s, self.prev_t = s, now
            self.done_n = 0
            if eff >= self.target - _EPS_NM:
                self._log(f"두께 판정 복귀 — 이미 목표 이상(유효 {eff:.1f} / {self.target:.1f} nm) → 종료")
                return ThicknessEvent("done", reason="복귀 시 이미 목표 이상", eff_nm=eff)
            self._log(f"두께 판정 복귀 — 유효 {eff:.1f} / {self.target:.1f} nm")
            return ThicknessEvent("resume", reason="통신 복귀", eff_nm=eff)

        if self.base is None:
            if not self._take_base(s, st, now):
                return None                  # 영점 확인 대기 — 이 값은 판정에 쓰지 않는다

        eff = s - self.base
        dt = max(1e-3, now - float(self.prev_t))
        d = s - float(self.prev_s)
        self.prev_s, self.prev_t = s, now
        self._set_eff(eff, now)
        self._warnings(st, eff, d, dt, now)

        if eff >= self.target - _EPS_NM:
            self.done_n += 1
            if self.done_n >= int(p.done_consecutive):
                self._log(f"목표 도달 — 유효 {eff:.2f} / {self.target:.1f} nm (연속 {self.done_n}회)")
                return ThicknessEvent("done", reason="목표 도달", eff_nm=eff)
        else:
            self.done_n = 0
        return None

    def _take_base(self, s: float, st: dict, now: float) -> bool:
        """기준점을 잡는다(잡았으면 True — 이 값부터 판정). 영점 실패면 첫 값, 성공이면 영점 반영 확인 뒤 0."""
        p = self.p
        if not self.zero_ok:
            self.base, self.base_t = s, now
            self._log(f"두께 기준점 {s:.2f} nm (영점 실패 — 영점 뒤 첫 값)")
        else:
            el = max(0.0, now - self.t0)
            r = st.get("rate_nm_s")
            rate = max(self.dep_rate, float(r) if isinstance(r, (int, float)) and r > 0 else 0.0)
            lim = float(p.zero_confirm_nm) + rate * el * 1.5
            if s <= lim:
                self.base, self.base_t = 0.0, self.t0
                self._log(f"영점 확인 — 영점 뒤 {el:.1f}초 값 {s:.2f} nm (≤ {lim:.2f}) → 기준점 0 nm(영점 시각)")
            elif el >= float(p.zero_confirm_s):
                self.base, self.base_t = s, now
                self._log(f"⚠ 영점 확인 안 됨 — {p.zero_confirm_s:g}초 동안 영점 반영 값이 없음, 현재 값 {s:.2f} nm 를 기준점으로")
            else:
                return False
        self.prev_s, self.prev_t = s, now
        self._set_eff(s - self.base, now)
        self._last_inc_t = now
        return True

    def _warnings(self, st: dict, eff: float, d: float, dt: float, now: float) -> None:
        """기록만 — 판정에 쓰지 않는다."""
        p = self.p
        lim = max(float(p.jump_min_nm), self.dep_rate * float(p.jump_rate_k) * dt)
        if d > lim:
            self.jump_count += 1
            self._log(f"⚠ 큰 튐 +{d:.2f} nm / {dt:.1f}초 (기준 {lim:.2f} nm) — 기록만, 판정은 장비 두께 그대로 "
                      f"(유효 {eff:.1f} nm, 이번 런 {self.jump_count}회)")
        if d > 0:
            self._last_inc_t = now
            self._zero_warned = False
        elif self._last_inc_t is not None and not self._zero_warned and now - self._last_inc_t >= float(p.rate_zero_warn_s):
            self._zero_warned = True
            self._log(f"⚠ 두께가 {now - self._last_inc_t:.0f}초 동안 늘지 않음(증착률 0) — 기록만, 공정은 계속")
        r = st.get("rate_nm_s")
        if isinstance(r, (int, float)) and self.dep_rate > 0:
            if r > self.dep_rate * float(p.rate_spike_k):
                if not self._spike_on:
                    self._spike_on = True
                    self._log(f"⚠ 증착률 급등 {r:.3f} nm/s (레시피 {self.dep_rate:g} × {p.rate_spike_k:g} 초과) — 기록만")
            elif r < self.dep_rate * float(p.rate_spike_k) * 0.5:
                self._spike_on = False
        if eff < -float(p.neg_warn_nm) and not self._neg_warned:
            self._neg_warned = True
            self._log(f"⚠ 유효두께 음수 {eff:.2f} nm — 기록만")

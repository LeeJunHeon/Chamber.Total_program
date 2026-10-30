# controller/thickness_process.py
# -*- coding: utf-8 -*-
"""두께 모니터 공정 연동 — Qt·asyncio 비의존 순수 판정. HeaterSoakWaiter 처럼 상태 dict 를 받아 tick() 한다.

모드 판정(thickness_mode_of)은 이 한 곳에 둔다 — 런타임(시작·시퀀스)과 ETA 계산이 같은 규칙을 쓴다.

순서(Thickness 모드): Shutter Delay → THICKNESS_ZERO(영점 B + ZeroConfirm) → Main Shutter 열기 → THICKNESS_WAIT(ThicknessEndJudge)

ZeroConfirm.tick(status, now) → None | ZeroResult — 메인 셔터를 열기 전에 기준점을 정한다.
  - 영점 성공이면 영점 뒤 새 값 ≤ zero_confirm_nm(음수 포함)을 한 번 보면 영점 확인 → 기준점 0.
  - 영점 실패면 영점 뒤 첫 새 값을 기준점으로 + 경고.
  - zero_confirm_s 안에 확인이 안 되면 마지막 새 값을 기준점으로 + 경고(영점이 안 먹음). 새 값이 하나도 없으면 기준점 없음
    (통신 끊김 — 판정기는 처음부터 영구 시간 대체).
  확인 전 값(영점 반영 전 남은 이전 두께)은 판정에 쓰이지 않는다 — 셔터가 열리기 전에 끝나기 때문.

ThicknessEndJudge.tick(status, now) → None | ThicknessEvent  (셔터를 연 뒤 첫 새 값부터)
  done     : 목표 도달(정상 종료). 복귀 순간 이미 목표 이상이어도 done.
  fallback : '시간 대체' 로 전환 — remaining_s 만큼 더 증착하고 끝낸다(타이머는 공정 단계가 가진다).
             permanent=True(기준점 없음·XTAL FAIL·공정 중 리셋·판정 예외)면 두께 판정으로 돌아오지 않는다.
  resume   : 통신이 돌아와(리셋 없음) 두께 판정으로 복귀 — 공정 단계가 시간 대체 타이머를 지운다.
판정 규칙(STM-100 이 초당 4회 두께를 계산해 주므로 그 값으로 끊는다 — Evaporator 실제 공정과 같은 방식):
  - 유효두께 = 장비 두께 − 기준점(ZeroConfirm 결과). 셔터 열림 뒤 하락(열 충격)·회복은 장비 두께 그대로(잠깐 음수였다 돌아옴).
  - status.seq 가 바뀐 값만 판정한다(같은 값 중복 금지).
  - 종료: 유효두께 ≥ 목표 인 새 값이 done_consecutive 번 연속(기본 1 = 도달 즉시). 하나라도 목표 미만이면 다시 센다.
  - stale_s 동안 새 값이 없으면 시간 대체. 남은 시간 = (목표 − 마지막 유효두께) ÷ 사용 증착률 − (지금 − 마지막 값 받은 시각)
    (셔터 연 뒤 측정 measured_rate_min_s 이상이고 실측 평균이 레시피 dep_rate 의 RATE_SANE_MIN~MAX 배 안이면 실측, 아니면 레시피).
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
END_ABORT = "중단"                # 컨트롤러가 정지 때 쓰는 말과 같게(THICKNESS_WAIT 중단 · 메인 공정 전 종료)
_EPS_NM = 1e-6                   # Å→nm 환산(×0.1)·기준점 뺄셈의 부동소수점 오차 — 100.0 nm 가 99.9999999 로 읽혀 한 번 늦게 끝나지 않게
RATE_SANE_MIN = 0.5              # 시간 대체 실측 증착률 상식 범위(레시피 dep_rate 의 배수) — 밖이면 레시피 값
RATE_SANE_MAX = 2.0


def _f(v) -> float:
    try:
        return float(str(v).strip()) if v is not None and str(v).strip() else 0.0
    except Exception:
        return 0.0


def xtal_fail(st: dict) -> bool:
    """크리스탈 FAIL 이 확인됨 — 통신은 정상(link_ok)인데 M 이 '!'. 값 없음(stale)이어도 본다.
    link_ok 가 없는 상태 dict(이전 형식)는 'not stale' 로 대신한다."""
    link = st.get("link_ok") if "link_ok" in st else not st.get("stale")
    return st.get("crystal_ok") is False and bool(link)


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


# ═══════════════════════ 영점 확인(메인 셔터 열기 전) ═══════════════════════
@dataclass
class ZeroResult:
    base: Optional[float]                # 기준점(nm). None = 새 값을 하나도 못 받음(기준점 없음)
    log: str                             # 공정 로그 한 줄
    warning: str = ""                    # 이번 런 경고 목록(종료 카드)으로 갈 문구


class ZeroConfirm:
    def __init__(self, p: ThicknessJudgeParams, *, start_t: float, seq0: Any, zero_ok: bool):
        self.p = p
        self.t0 = float(start_t)
        self._seq_last = seq0
        self.zero_ok = bool(zero_ok)
        self.last: Optional[float] = None    # 영점 뒤 마지막 새 값

    def tick(self, st: dict, now: float) -> Optional[ZeroResult]:
        p = self.p
        if xtal_fail(st):                    # 기다려도 값이 오지 않는다 — 바로 기준점 없음
            w = "기준점 없음 — XTAL FAIL(크리스탈 이상), 시간으로 진행"
            return ZeroResult(None, f"⚠ {w}", w)
        seq, s = st.get("seq"), st.get("thickness_nm")
        if seq is not None and seq != self._seq_last and s is not None:
            self._seq_last = seq
            s = float(s)
            self.last = s
            if not self.zero_ok:
                return ZeroResult(s, f"⚠ 영점 명령 실패 — 첫 값 {s:.2f} nm 를 기준점으로",
                                  f"영점 명령 실패 — 첫 값 {s:.2f} nm 를 기준점으로")
            if s <= float(p.zero_confirm_nm):
                return ZeroResult(0.0, f"영점 확인 — 영점 뒤 {now - self.t0:.1f}초 값 {s:.2f} nm "
                                       f"(≤ {p.zero_confirm_nm:g}) → 기준점 0 nm")
        if now - self.t0 < float(p.zero_confirm_s):
            return None
        if self.last is None:
            w = f"영점 확인 중 두께 값 없음({p.zero_confirm_s:g}초) — 기준점 없음, 시간으로 진행"
            return ZeroResult(None, f"⚠ {w}", w)
        if self.zero_ok:
            w = f"영점 확인 안 됨 — {self.last:.2f} nm 를 기준점으로({p.zero_confirm_s:g}초 동안 ≤ {p.zero_confirm_nm:g} nm 값 없음)"
        else:
            w = f"영점 명령 실패 — {self.last:.2f} nm 를 기준점으로"
        return ZeroResult(self.last, f"⚠ {w}", w)


# ═══════════════════════ 종료 판정(메인 셔터 연 뒤) ═══════════════════════
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
                 start_t: float, base: Optional[float], seq0: Any = 0, reset0: int = 0,
                 log: Optional[Callable[[str], None]] = None):
        self.target = float(target_nm)
        self.dep_rate = float(dep_rate_nm_s)
        self.p = p
        self.t0 = float(start_t)             # 메인 셔터를 연 시각(실측 평균 증착률의 기준)
        self._seq_last = seq0
        self._reset0 = int(reset0 or 0)
        self._log_cb = log
        self.events: List[str] = []          # 로그 문구(테스트·기록)

        self.mode = "thickness"              # "thickness" | "fallback"
        self.permanent = False
        self.fallback_reason = ""            # 마지막 시간 대체 전환 사유
        self.fallback_count = 0
        self.base: Optional[float] = None if base is None else float(base)
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
    def start_event(self) -> Optional[ThicknessEvent]:
        """기준점이 없으면(영점 확인 중 통신 끊김) 처음부터 영구 시간 대체 — 런타임이 판정기를 만든 직후 부른다."""
        if self.base is not None:
            return None
        return self._to_fallback(self.t0, "영점 확인 중 두께 값 없음 — 기준점 없음", permanent=True)

    def tick(self, st: dict, now: float) -> Optional[ThicknessEvent]:
        try:
            return self._tick(st or {}, float(now))
        except Exception as e:
            tb = traceback.format_exc()
            return self._to_fallback(now, f"판정 코드 예외({e!r})", permanent=True, detail=tb)

    def measured_rate(self) -> Optional[float]:
        """실측 평균 증착률(유효두께 ÷ 셔터 연 뒤 경과) — 측정 measured_rate_min_s 미만·두께 0 이하면 None."""
        if self.eff_t is not None:
            span = self.eff_t - self.t0
            if span >= float(self.p.measured_rate_min_s) and self.eff > 0:
                return self.eff / span
        return None

    def use_rate(self) -> Tuple[float, str]:
        """시간 대체 증착률 — (nm/s, '실측'|'레시피'). 실측이 레시피 dep_rate 의 RATE_SANE_MIN~MAX 배 밖이면 레시피."""
        m = self.measured_rate()
        if m is not None and (self.dep_rate <= 0 or RATE_SANE_MIN * self.dep_rate <= m <= RATE_SANE_MAX * self.dep_rate):
            return m, "실측"
        return self.dep_rate, "레시피"

    def fallback_remaining_s(self, now: Optional[float] = None) -> float:
        """시간 대체 남은 시간. now 를 주면 마지막 유효두께를 받은 뒤 이미 지난 시간을 뺀다(eff_t 가 없으면 빼지 않음)."""
        rate, _src = self.use_rate()
        left = max(0.0, self.target - self.eff)
        rem = left / rate if rate > 0 else left / max(self.dep_rate, 1e-9)
        if now is not None and self.eff_t is not None:
            rem = max(0.0, rem - max(0.0, float(now) - self.eff_t))
        return rem

    def avg_rate(self) -> Optional[float]:
        """평균 증착률(유효두께 ÷ 셔터 연 뒤 경과) — 기록용."""
        if self.eff_t is None or self.eff_t <= self.t0:
            return None
        return self.eff / (self.eff_t - self.t0)

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
            rem = self.fallback_remaining_s(now)
            rate, src = self.use_rate()
            how = f"(목표 {self.target:.1f} − 유효두께 {self.eff:.1f} nm) ÷ {src} {rate:.4f} nm/s"
            m = self.measured_rate()
            if src == "레시피" and m is not None:
                how += (f" (실측 {m:.4f} nm/s 가 레시피의 {RATE_SANE_MIN:g}~{RATE_SANE_MAX:g}배 밖 → 레시피 사용)")
            if self.eff_t is not None:
                how += f" − 마지막 값 뒤 {max(0.0, float(now) - self.eff_t):.0f}초 반영"
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
        if xtal_fail(st):
            return self._to_fallback(now, "XTAL FAIL(크리스탈 이상)", permanent=True)

        seq, s = st.get("seq"), st.get("thickness_nm")
        if seq is None or seq == self._seq_last or s is None:
            if self.mode == "thickness" and (now - self.last_new_t) > float(p.stale_s):
                return self._to_fallback(now, f"새 두께 값 없음 {now - self.last_new_t:.0f}초", permanent=False)
            return None
        self._seq_last = seq
        self.last_new_t = now
        s = float(s)
        eff = s - self.base

        if self.mode == "fallback":          # 통신 복귀(리셋 없음) — 장비는 끊긴 동안에도 두께를 누적했다
            self.mode = "thickness"
            self._set_eff(eff, now)
            self.prev_s, self.prev_t = s, now
            self.done_n = 0
            if eff >= self.target - _EPS_NM:
                self.done_n = 1                  # 복귀 첫 값도 같은 연속 횟수 규칙(값 하나가 튀어도 바로 끝내지 않게)
                if self.done_n >= int(p.done_consecutive):
                    self._log(f"두께 판정 복귀 — 이미 목표 이상(유효 {eff:.1f} / {self.target:.1f} nm) → 종료")
                    return ThicknessEvent("done", reason="복귀 시 이미 목표 이상", eff_nm=eff)
                self._log(f"두께 판정 복귀 — 첫 값이 목표 이상(유효 {eff:.1f} / 목표 {self.target:.1f} nm), 다음 값으로 확인")
                return ThicknessEvent("resume", reason="통신 복귀(목표 이상 — 다음 값으로 확인)", eff_nm=eff)
            self._log(f"두께 판정 복귀 — 유효 {eff:.1f} / {self.target:.1f} nm")
            return ThicknessEvent("resume", reason="통신 복귀", eff_nm=eff)

        if self.prev_s is not None:
            dt = max(1e-3, now - float(self.prev_t))
            self._warnings(st, eff, s - float(self.prev_s), dt, now)
        else:
            self._last_inc_t = now
        self.prev_s, self.prev_t = s, now
        self._set_eff(eff, now)

        if eff >= self.target - _EPS_NM:
            self.done_n += 1
            if self.done_n >= int(p.done_consecutive):
                self._log(f"목표 도달 — 유효 {eff:.2f} / {self.target:.1f} nm (연속 {self.done_n}회)")
                return ThicknessEvent("done", reason="목표 도달", eff_nm=eff)
        else:
            self.done_n = 0
        return None

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

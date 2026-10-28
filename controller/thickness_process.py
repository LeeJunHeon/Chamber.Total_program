# controller/thickness_process.py
# -*- coding: utf-8 -*-
"""두께 모니터 공정 연동(2단계) — Qt·asyncio 비의존 순수 판정. HeaterSoakWaiter 처럼 상태 dict 를 받아 tick() 한다.

모드 판정(thickness_mode_of)은 이 한 곳에 둔다 — 런타임(시작·시퀀스)과 ETA 계산이 같은 규칙을 쓴다.

ThicknessEndJudge.tick(status, now) → None | ThicknessEvent
  done     : 목표 도달(정상 종료). 복귀 순간 이미 목표 이상이어도 done.
  fallback : '시간 대체' 로 전환 — remaining_s 만큼 더 증착하고 끝낸다(타이머는 공정 단계가 가진다).
             permanent=True(XTAL FAIL·공정 중 리셋·판정 예외)면 두께 판정으로 돌아오지 않는다.
  resume   : 통신이 돌아와(리셋 없음) 두께 판정으로 복귀 — 공정 단계가 시간 대체 타이머를 지운다.
판정 규칙:
  - 영점 뒤 처음 받은 새 S 를 기준점으로(유효두께 = S − 기준점). 기준점 뒤 zero_late_window_s 안에 zero_late_drop_nm 이상
    떨어지면 영점 지연 반영으로 보고 기준점 재설정. 기준점 뒤 step_skip_s 는 계단 튐 판정에서 뺀다(기준 증착률 표본은 모은다).
  - status.seq 가 바뀐 값만 판정한다(같은 값 중복 금지).
  - 정상 종료: 유효두께 ≥ 목표 인 새 값이 done_consecutive 번 연속.
  - 계단 튐: 증가량 > max(기준 증착률 × step_rate_k × Δt, step_min_nm) 이면 의심(기준 = 최근 정상 증가율 ref_rate_n 개의 중앙값,
    5개 미만이면 레시피 dep_rate — 의심 중에는 기준을 그 시작 값으로 고정). 의심 동안 한계 초과 증가의 초과분
    (d − 기준×Δt)을 모은다. 의심 중에는 끝내지 않는다. 끝나는 방법은 셋(Evaporator stm_shadow 실제 로그 재생 규칙):
      (a) 한계 초과 증가가 step_elev_s 이상 쌓임(값이 떨어지지 않고) → 실제 증착률 변화. 제외 없음, 기준 표본을 그 구간의
          증가율로 다시 채움. 그 순간 목표 이상이면 바로 종료.
      (b) 정상 증가 step_normal_n 번 → 한 번 튄 측정 오류. 모은 초과분을 유효두께에서 뺀다(기준점 += 초과분),
          그 뒤 종료는 done_consecutive 규칙 그대로. 제외 합계가 목표 × step_exclude_warn_frac 를 넘으면 경고 1회.
      (c) 값이 다시 떨어짐 → 의심 취소, 제외 없이 계속.
  - stale_s 동안 새 값이 없으면 시간 대체. 남은 시간 = (목표 − 마지막 정상 유효두께) ÷ 사용 증착률
    (측정 measured_rate_min_s 이상이면 실측 평균, 아니면 레시피 dep_rate). 기준점을 잡기 전에 끊겼다가 돌아오면
    영점이 성공했으면 기준점 0(영점 시각), 아니면 기준을 알 수 없으므로 영구 시간 대체.
  - 증착률 0 지속·급등·두께 음수는 경고 기록만.
  - 판정 코드 예외는 삼키지 않고 곧바로 영구 시간 대체(traceback 은 event.detail).
"""
from __future__ import annotations

import statistics
import traceback
from collections import deque
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
        self.inc = deque(maxlen=max(5, int(p.ref_rate_n)))
        self.done_n = 0
        self.suspect: Optional[dict] = None
        self.suspect_count = 0
        self.step_excluded_nm = 0.0          # (b) 로 뺀 계단 튐 합계
        self._exclude_warned = False
        self._notices: List[str] = []        # 구글챗으로 알릴 문구(런타임이 pop_notices 로 가져간다)
        self.eff = 0.0                       # 마지막 유효두께(의심 값 포함)
        self.good_eff = 0.0                  # 마지막 '정상' 유효두께(시간 대체 계산 기준)
        self.good_t: Optional[float] = None
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

    def pop_notices(self) -> List[str]:
        out, self._notices = self._notices, []
        return out

    def use_rate(self) -> Tuple[float, str]:
        """시간 대체 증착률 — (nm/s, '실측'|'레시피')."""
        if self.good_t is not None and self.base_t is not None:
            span = self.good_t - self.base_t
            if span >= float(self.p.measured_rate_min_s) and self.good_eff > 0:
                return self.good_eff / span, "실측"
        return self.dep_rate, "레시피"

    def fallback_remaining_s(self) -> float:
        rate, _src = self.use_rate()
        left = max(0.0, self.target - self.good_eff)
        return left / rate if rate > 0 else left / max(self.dep_rate, 1e-9)

    def avg_rate(self) -> Optional[float]:
        """평균 증착률(정상 유효두께 ÷ 기준점 뒤 경과) — 기록용."""
        if self.good_t is None or self.base_t is None or self.good_t <= self.base_t:
            return None
        return self.good_eff / (self.good_t - self.base_t)

    def ref_rate(self) -> float:
        if len(self.inc) >= 5:
            return max(0.0, statistics.median(self.inc))
        return self.dep_rate

    def view(self, st: dict, now: float) -> Tuple[str, float]:
        """상태 문구·진행률(%) — "두께 x.x / y.y nm (nn%) · 증착률 r nm/s · 예상 남은 시간 mm:ss"."""
        eff = self.eff
        pct = max(0.0, eff / self.target * 100.0) if self.target > 0 else 0.0
        if self.mode == "fallback":
            rate, _src = self.use_rate()
        else:
            r = st.get("rate_nm_s")
            rate = float(r) if isinstance(r, (int, float)) and r > 0 else self.ref_rate()
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

    def _good(self, eff: float, now: float) -> None:
        self.good_eff = eff
        self.good_t = now

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
        self.suspect = None
        try:
            rem = self.fallback_remaining_s()
            rate, src = self.use_rate()
            how = f"(목표 {self.target:.1f} − 정상 유효두께 {self.good_eff:.1f} nm) ÷ {src} {rate:.4f} nm/s"
        except Exception:
            rem, how = self.target / max(self.dep_rate, 1e-9), "레시피 예상시간"
        self._log(f"⚠ 시간 대체로 전환 — {reason}. 남은 시간 {rem:.0f}초 = {how}"
                  + ("" if permanent else " · 통신이 돌아오면 두께 판정으로 복귀"))
        return ThicknessEvent("fallback", reason=reason, remaining_s=rem, permanent=bool(permanent),
                              eff_nm=self.good_eff, detail=detail)

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
            self.eff = eff
            self.prev_s, self.prev_t = s, now
            self.done_n = 0
            self.suspect = None
            self._good(eff, now)
            if eff >= self.target - _EPS_NM:
                self._log(f"두께 판정 복귀 — 이미 목표 이상(유효 {eff:.1f} / {self.target:.1f} nm) → 종료")
                return ThicknessEvent("done", reason="복귀 시 이미 목표 이상", eff_nm=eff)
            self._log(f"두께 판정 복귀 — 유효 {eff:.1f} / {self.target:.1f} nm")
            return ThicknessEvent("resume", reason="통신 복귀", eff_nm=eff)

        if self.base is None:
            self.base, self.base_t = s, now
            self.prev_s, self.prev_t = s, now
            self.eff = 0.0
            self._good(0.0, now)
            self._last_inc_t = now
            self._log(f"두께 기준점 {s:.2f} nm (영점 뒤 첫 값)")
            return None

        if (now - self.base_t) <= float(p.zero_late_window_s) and (self.base - s) >= float(p.zero_late_drop_nm):
            self._log(f"영점 지연 반영 감지({s - self.base:+.2f} nm, 기준점 뒤 {now - self.base_t:.1f}초) → 기준점 재설정 {s:.2f} nm")
            self.base, self.base_t = s, now
            self.prev_s, self.prev_t = s, now
            self.inc.clear()
            self.suspect = None
            self.done_n = 0
            self.eff = 0.0
            self._good(0.0, now)
            return None

        eff = s - self.base
        self.eff = eff
        dt = max(1e-3, now - float(self.prev_t))
        d = s - float(self.prev_s)
        in_skip = (now - self.base_t) < float(p.step_skip_s)
        thr = max(self.ref_rate() * float(p.step_rate_k) * dt, float(p.step_min_nm))
        self.prev_s, self.prev_t = s, now
        self._warnings(st, eff, d, now)

        if self.suspect is not None:
            return self._suspect_step(eff, d, dt, now)

        if not in_skip and d > thr:
            ref = self.ref_rate()
            self.suspect = {"normal": 0, "ref": ref, "excess": d - ref * dt, "elev_s": dt, "rates": [d / dt],
                            "t": now}
            self.suspect_count += 1
            self.done_n = 0
            self._log(f"⚠ 계단 튐 의심 +{d:.2f} nm / {dt:.1f}초 (한계 {thr:.2f} nm = max(기준 {ref:.4f} nm/s × "
                      f"{p.step_rate_k:g} × Δt, {p.step_min_nm:g} nm)) — 유효 {eff:.1f} nm, 끝내지 않고 확인")
            return None

        if d >= 0:
            # 기준 증착률 표본은 처음 step_skip_s 동안에도 모은다(튐 판정만 뺀다) — 실제가 레시피보다 훨씬 빠를 때
            # 분해능(0.1 nm) 흔들림 한 번이 레시피 기준으로 '튐'이 되어 실제 증착이 제외되던 문제
            self.inc.append(d / dt)
        self._good(eff, now)
        if eff >= self.target - _EPS_NM:
            self.done_n += 1
            if self.done_n >= int(p.done_consecutive):
                self._log(f"목표 도달 — 유효 {eff:.2f} / {self.target:.1f} nm (연속 {self.done_n}회)")
                return ThicknessEvent("done", reason="목표 도달", eff_nm=eff)
        else:
            self.done_n = 0
        return None

    def _suspect_step(self, eff: float, d: float, dt: float, now: float) -> Optional[ThicknessEvent]:
        """계단 튐 의심 중 새 값 — (a) 지속 = 증착률 변화 / (b) 정상 증가 n 회 = 측정 오류 제외 / (c) 다시 떨어짐 = 취소."""
        p = self.p
        sp = self.suspect
        ref = float(sp["ref"])                                   # 의심 동안 기준 고정(경계 증착률에서 한계가 흔들리지 않게)
        thr = max(ref * float(p.step_rate_k) * dt, float(p.step_min_nm))
        if d < -thr:                                             # (c)
            self.suspect = None
            self.done_n = 0
            self._log(f"계단 튐 의심 취소 — 값이 다시 떨어짐({d:+.2f} nm, 유효 {eff:.1f} nm) → 제외 없이 계속")
            self._good(eff, now)
            return None
        if d >= 0:
            sp["rates"].append(d / dt)
        if d > thr:
            sp["excess"] += d - ref * dt
            sp["elev_s"] += dt
            sp["normal"] = 0
            if sp["elev_s"] < float(p.step_elev_s):
                return None
            # (a) 실제 증착률 변화 — 제외 없음, 기준 표본을 이 구간의 증가율로 다시 채운다
            self.suspect = None
            self.inc.clear()
            self.inc.extend(sp["rates"][-self.inc.maxlen:])
            self._good(eff, now)
            self._log(f"증착률 변화로 판정(계단 튐 아님) — 한계 초과 {sp['elev_s']:.1f}초 지속, "
                      f"새 기준 {self.ref_rate():.4f} nm/s, 유효 {eff:.1f} nm")
            if eff >= self.target - _EPS_NM:
                self._log(f"목표 도달 — 유효 {eff:.2f} / {self.target:.1f} nm (증착률 변화 확인 시점)")
                return ThicknessEvent("done", reason="증착률 변화 확인 — 목표 이상", eff_nm=eff)
            self.done_n = 0
            return None
        sp["normal"] += 1
        if sp["normal"] < int(p.step_normal_n):
            return None
        # (b) 한 번 튄 측정 오류 — 모은 초과분을 뺀다(기준점 += 초과분)
        self.suspect = None
        ex = max(0.0, float(sp["excess"]))
        if ex > 0:
            self.base += ex
            eff -= ex
            self.eff = eff
            self.step_excluded_nm += ex
            self._log(f"계단 튐 제외 {ex:.2f} nm — 정상 증가 {p.step_normal_n}회 확인, 유효 {eff:.1f} / {self.target:.1f} nm "
                      f"(제외 합계 {self.step_excluded_nm:.2f} nm)")
            lim = self.target * float(p.step_exclude_warn_frac)
            if not self._exclude_warned and self.step_excluded_nm > lim:
                self._exclude_warned = True
                msg = (f"계단 튐 제외 합계 {self.step_excluded_nm:.2f} nm — 목표 {self.target:g} nm 의 "
                       f"{p.step_exclude_warn_frac * 100:g}% 초과, 확인 필요")
                self._log(f"⚠ {msg}")
                self._notices.append(msg)
        else:
            self._log(f"계단 튐 의심 해제 — 정상 증가 {p.step_normal_n}회(유효 {eff:.1f} nm, 제외 없음)")
        self._good(eff, now)
        self.done_n = 1 if eff >= self.target - _EPS_NM else 0
        if self.done_n >= int(p.done_consecutive):
            self._log(f"목표 도달 — 유효 {eff:.2f} / {self.target:.1f} nm")
            return ThicknessEvent("done", reason="목표 도달", eff_nm=eff)
        return None

    def _warnings(self, st: dict, eff: float, d: float, now: float) -> None:
        p = self.p
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
        if eff < -float(p.zero_late_drop_nm) and not self._neg_warned:
            self._neg_warned = True
            self._log(f"⚠ 유효두께 음수 {eff:.2f} nm — 기록만")

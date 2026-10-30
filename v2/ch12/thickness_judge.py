"""챔버12 두께 판정(순수 — 장비·스레드·시계 없음): 판정 상수·모드·영점 확인(ZeroConfirm)·종료 판정(ThicknessEndJudge).

v1 근거(읽고 다시 씀, import 안 함): controller/thickness_process.py(전부), lib/thickness_config.py:44-100(ThicknessJudgeParams·
load_judge_params). 실행(공정 스텝·시간 대체 타이머·상한)은 thickness.py(ThicknessEngine).

순서(Thickness 모드): Shutter Delay → THICKNESS_ZERO(영점 B + ZeroConfirm) → Main Shutter 열기 → THICKNESS_WAIT(ThicknessEndJudge)

ZeroConfirm.tick(status, now) → None | ZeroResult — 메인 셔터를 열기 전에 기준점을 정한다.
  - XTAL FAIL(통신 정상)이면 바로 기준점 없음.
  - 영점 성공이면 영점 뒤 새 값(seq 변화) ≤ zero_confirm_nm(음수 포함)을 한 번 보면 기준점 0.
  - 영점 실패면 영점 뒤 첫 새 값을 기준점 + 경고.
  - zero_confirm_s 안에 확인이 안 되면 마지막 새 값을 기준점 + 경고, 새 값이 하나도 없으면 기준점 없음
    (판정기는 처음부터 영구 시간 대체).
ThicknessEndJudge.tick(status, now) → None | ThicknessEvent  (셔터를 연 뒤 첫 새 값부터)
  done     : 유효두께(장비 두께 − 기준점) ≥ 목표 − 1e-6 인 새 값이 done_consecutive 번 연속(CH2 운전값 2 — 15차 88455cf,
             늦은 응답이 두께로 읽혀 값 하나가 튀어도 끝내지 않게). 하나라도 미만이면 다시 센다. 같은 seq 는 보지 않는다.
  fallback : '시간 대체' — remaining_s 뒤 끝낸다(타이머는 실행 쪽). 새 값이 stale_s(10 s) 넘게 없으면(복귀 가능),
             기준점 없음·XTAL FAIL·공정 중 리셋 플래그·판정 예외는 permanent(두께 판정으로 돌아오지 않음).
  resume   : 시간 대체 중 새 값 — 두께 판정으로 복귀(연속 횟수 처음부터). 첫 값이 이미 목표 이상이면 연속 1로 세고
             필요 횟수가 1이면 done('복귀 시 이미 목표 이상'), 아니면 resume 하고 다음 값으로 확인.
  남은 시간(시간 대체) = max(0, 목표 − 마지막 유효두께) ÷ 사용 증착률 − (지금 − 마지막 값 시각). 사용 증착률 = 셔터 연 뒤 측정
  measured_rate_min_s(30 s) 이상·유효두께 > 0 이면 실측 평균, 실측이 레시피 dep_rate 의 0.5~2.0배 밖이면 레시피.
  기록만(판정 무관): 큰 튐(증가량 > max(jump_min, dep_rate×jump_k×Δt)), 60 s 늘지 않음, 증착률 급등(×5, ×2.5 미만 해제), 음수.

v2 에서 더한 것(결정 목록 '재시작 이어가기' — 판정 규칙은 그대로): ThicknessEndJudge.to_state()/from_state() 는 저널용 상태를
  메인 셔터 연 시각(t0) 기준 상대 시각으로 꺼내고 되살린다(재시작 뒤 단조 시계가 달라도 같은 판정).
"""
from __future__ import annotations

import traceback
from dataclasses import asdict, dataclass, fields
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

END_THICKNESS = "Thickness"      # 종료 방식(공정 기록·카드 표기) — controller/thickness_process.py:39-43
END_FALLBACK = "시간대체"
END_LIMIT = "상한"
END_TIME = "Time"
END_ABORT = "중단"
EPS_NM = 1e-6                    # :44 — Å→nm(×0.1)·기준점 뺄셈 부동소수점 오차(100.0 nm 가 99.9999999 로 읽혀 늦게 끝나지 않게)
RATE_SANE_MIN = 0.5              # :45-46 — 시간 대체 실측 증착률 상식 범위(레시피 dep_rate 배수)
RATE_SANE_MAX = 2.0


@dataclass(frozen=True)
class JudgeParams:
    """v1 ThicknessJudgeParams(lib/thickness_config.py:44-61) — 기본값 = v1 클래스 기본(공통 DONE_CONSECUTIVE 1)."""
    stale_s: float = 10.0
    ready_fresh_s: float = 5.0
    life_warn_pct: float = 10.0
    max_time_ratio: float = 2.0
    max_time_margin_s: float = 300.0
    zero_confirm_nm: float = 2.0
    zero_confirm_s: float = 5.0
    neg_warn_nm: float = 2.0
    done_consecutive: int = 1
    jump_min_nm: float = 1.0
    jump_rate_k: float = 10.0
    measured_rate_min_s: float = 30.0
    rate_zero_warn_s: float = 60.0
    rate_spike_k: float = 5.0


# (속성, v1 키, 하한, 상한) — lib/thickness_config.py:84-99 의 클램프 범위
_JUDGE_KEYS = (
    ("stale_s", "THICKNESS_STALE_S", 2.0, 120.0), ("ready_fresh_s", "THICKNESS_READY_FRESH_S", 1.0, 60.0),
    ("life_warn_pct", "THICKNESS_LIFE_WARN_PCT", 0.0, 100.0), ("max_time_ratio", "THICKNESS_MAX_TIME_RATIO", 1.0, 10.0),
    ("max_time_margin_s", "THICKNESS_MAX_TIME_MARGIN_S", 0.0, 7200.0),
    ("zero_confirm_nm", "THICKNESS_ZERO_CONFIRM_NM", 0.1, 100.0), ("zero_confirm_s", "THICKNESS_ZERO_CONFIRM_S", 1.0, 60.0),
    ("neg_warn_nm", "THICKNESS_NEG_WARN_NM", 0.1, 100.0), ("done_consecutive", "THICKNESS_DONE_CONSECUTIVE", 1, 10),
    ("jump_min_nm", "THICKNESS_JUMP_MIN_NM", 0.1, 100.0), ("jump_rate_k", "THICKNESS_JUMP_RATE_K", 1.0, 1000.0),
    ("measured_rate_min_s", "THICKNESS_MEASURED_RATE_MIN_S", 5.0, 3600.0),
    ("rate_zero_warn_s", "THICKNESS_RATE_ZERO_WARN_S", 5.0, 3600.0), ("rate_spike_k", "THICKNESS_RATE_SPIKE_K", 1.5, 100.0),
)


def chamber_no(chamber: Any) -> int:
    return int(str(chamber).lower().replace("ch", ""))


def cfg_get(cfg: Mapping[str, Any], chamber: Any, key: str, default: Any = None) -> Any:
    """채널 키(CHn_KEY) → 공통 키(KEY) → default(v2 파라미터 규칙, process_steps._cget 와 같음)."""
    v = cfg.get(f"CH{chamber_no(chamber)}_{key}")
    if v is None:
        v = cfg.get(key)
    return default if v is None else v


def judge_params(cfg: Mapping[str, Any], chamber: Any) -> Tuple[JudgeParams, List[str]]:
    """v1 load_judge_params — 형식 오류(bool 포함) → 기본값 + 경고, 범위 밖 → 클램프 + 경고. 예외를 던지지 않는다.
    부르는 때마다 읽는다(v1 도 쓰는 때 load_judge_params 를 다시 불렀다)."""
    d = JudgeParams()
    warns: List[str] = []
    out: Dict[str, Any] = {}
    for attr, key, lo, hi in _JUDGE_KEYS:
        default = getattr(d, attr)
        raw = cfg_get(cfg, chamber, key, default)
        try:
            if isinstance(raw, bool):
                raise TypeError
            v = float(raw)
        except Exception:
            warns.append(f"{key}={raw!r} 형식 오류 → 기본값 {default!r}")
            v = float(default)
        else:
            if not (lo <= v <= hi):
                nv = min(max(v, lo), hi)
                warns.append(f"{key}={v:g} 범위({lo:g}~{hi:g}) 밖 → {nv:g}")
                v = nv
        out[attr] = int(v) if attr == "done_consecutive" else v
    return JudgeParams(**out), warns


def _f(v) -> float:
    try:
        return float(str(v).strip()) if v is not None and str(v).strip() else 0.0
    except Exception:
        return 0.0


def xtal_fail(st: Mapping[str, Any]) -> bool:
    """XTAL FAIL 확인 — 통신 정상(link_ok, 없으면 not stale)인데 M 이 '!'. 값 없음(stale)이어도 본다(:56-60)."""
    link = st.get("link_ok") if "link_ok" in st else not st.get("stale")
    return st.get("crystal_ok") is False and bool(link)


def thickness_mode_of(thickness: Any, supported: bool) -> bool:
    """Thickness 모드 = thickness > 0 이고 그 채널이 두께 모니터를 씀(:63-67). process_time 칸은 보지 않는다."""
    return bool(supported) and _f(thickness) > 0.0


def expected_seconds(thickness_nm: float, dep_rate_nm_s: float) -> float:
    return float(thickness_nm) / float(dep_rate_nm_s)


def max_time_s(expected_s: float, p: JudgeParams) -> float:
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
    warning: str = ""                    # 이번 런 경고 목록(종료 카드)


class ZeroConfirm:
    def __init__(self, p: JudgeParams, *, start_t: float, seq0: Any, zero_ok: bool):
        self.p = p
        self.t0 = float(start_t)
        self._seq_last = seq0
        self.zero_ok = bool(zero_ok)
        self.last: Optional[float] = None

    def tick(self, st: Mapping[str, Any], now: float) -> Optional[ZeroResult]:
        p = self.p
        if xtal_fail(st):
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
    remaining_s: Optional[float] = None
    permanent: bool = False
    eff_nm: Optional[float] = None
    detail: str = ""                     # 판정 예외 traceback


# 저널로 꺼내는 판정기 상태 — 시각은 t0 기준 상대(초)
_STATE_PLAIN = ("target", "dep_rate", "mode", "permanent", "fallback_reason", "fallback_count", "base", "prev_s",
                "done_n", "jump_count", "eff", "_reset0", "_zero_warned", "_spike_on", "_neg_warned")
_STATE_TIMES = ("prev_t", "last_new_t", "eff_t", "_last_inc_t")


class ThicknessEndJudge:
    def __init__(self, target_nm: float, dep_rate_nm_s: float, p: JudgeParams, *,
                 start_t: float, base: Optional[float], seq0: Any = 0, reset0: int = 0,
                 log: Optional[Callable[[str], None]] = None):
        self.target = float(target_nm)
        self.dep_rate = float(dep_rate_nm_s)
        self.p = p
        self.t0 = float(start_t)             # 메인 셔터를 연 시각(실측 평균 증착률의 기준)
        self._seq_last = seq0
        self._reset0 = int(reset0 or 0)
        self._log_cb = log
        self.events: List[str] = []

        self.mode = "thickness"              # "thickness" | "fallback"
        self.permanent = False
        self.fallback_reason = ""
        self.fallback_count = 0
        self.base: Optional[float] = None if base is None else float(base)
        self.prev_s: Optional[float] = None
        self.prev_t: Optional[float] = None
        self.last_new_t = float(start_t)
        self.done_n = 0
        self.jump_count = 0
        self.eff = 0.0
        self.eff_t: Optional[float] = None
        self._last_inc_t: Optional[float] = None
        self._zero_warned = False
        self._spike_on = False
        self._neg_warned = False

    # ── 공개 ──
    def start_event(self) -> Optional[ThicknessEvent]:
        """기준점이 없으면 처음부터 영구 시간 대체 — 판정기를 만든 직후 부른다."""
        if self.base is not None:
            return None
        return self._to_fallback(self.t0, "영점 확인 중 두께 값 없음 — 기준점 없음", permanent=True)

    def tick(self, st: Mapping[str, Any], now: float) -> Optional[ThicknessEvent]:
        try:
            return self._tick(st or {}, float(now))
        except Exception as e:
            tb = traceback.format_exc()
            return self._to_fallback(now, f"판정 코드 예외({e!r})", permanent=True, detail=tb)

    def measured_rate(self) -> Optional[float]:
        if self.eff_t is not None:
            span = self.eff_t - self.t0
            if span >= float(self.p.measured_rate_min_s) and self.eff > 0:
                return self.eff / span
        return None

    def use_rate(self) -> Tuple[float, str]:
        m = self.measured_rate()
        if m is not None and (self.dep_rate <= 0 or RATE_SANE_MIN * self.dep_rate <= m <= RATE_SANE_MAX * self.dep_rate):
            return m, "실측"
        return self.dep_rate, "레시피"

    def fallback_remaining_s(self, now: Optional[float] = None) -> float:
        rate, _src = self.use_rate()
        left = max(0.0, self.target - self.eff)
        rem = left / rate if rate > 0 else left / max(self.dep_rate, 1e-9)
        if now is not None and self.eff_t is not None:
            rem = max(0.0, rem - max(0.0, float(now) - self.eff_t))
        return rem

    def avg_rate(self) -> Optional[float]:
        if self.eff_t is None or self.eff_t <= self.t0:
            return None
        return self.eff / (self.eff_t - self.t0)

    def view(self, st: Mapping[str, Any], now: float) -> Tuple[str, float]:
        """상태 문구·진행률(%) — '[시간 대체] 두께 x.x / y.y nm (nn%) · 증착률 r nm/s · 예상 남은 시간 mm:ss'."""
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

    # ── 저널(v2) ──
    def to_state(self) -> dict:
        d = {k: getattr(self, k) for k in _STATE_PLAIN}
        for k in _STATE_TIMES:
            v = getattr(self, k)
            d[k] = None if v is None else float(v) - self.t0
        d["params"] = asdict(self.p)
        return d

    @classmethod
    def from_state(cls, d: Mapping[str, Any], *, t0: float, seq0: Any, reset0: int,
                   log: Optional[Callable[[str], None]] = None) -> "ThicknessEndJudge":
        """저널 상태로 되살린다 — t0 = 새 단조 시계에서의 메인 셔터 연 시각, seq0·reset0 = 새 드라이버 기준."""
        names = {f.name for f in fields(JudgeParams)}
        p = JudgeParams(**{k: v for k, v in dict(d.get("params") or {}).items() if k in names})
        j = cls(d["target"], d["dep_rate"], p, start_t=t0, base=d.get("base"), seq0=seq0, reset0=reset0, log=log)
        for k in _STATE_PLAIN:
            if k in d and k not in ("target", "dep_rate", "base", "_reset0"):
                setattr(j, k, d[k])
        for k in _STATE_TIMES:
            v = d.get(k)
            setattr(j, k, None if v is None else t0 + float(v))
        return j

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

    def _tick(self, st: Mapping[str, Any], now: float) -> Optional[ThicknessEvent]:
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
            if eff >= self.target - EPS_NM:
                self.done_n = 1
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

        if eff >= self.target - EPS_NM:
            self.done_n += 1
            if self.done_n >= int(p.done_consecutive):
                self._log(f"목표 도달 — 유효 {eff:.2f} / {self.target:.1f} nm (연속 {self.done_n}회)")
                return ThicknessEvent("done", reason="목표 도달", eff_nm=eff)
        else:
            self.done_n = 0
        return None

    def _warnings(self, st: Mapping[str, Any], eff: float, d: float, dt: float, now: float) -> None:
        """기록만 — 판정에 쓰지 않는다(:324-348)."""
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

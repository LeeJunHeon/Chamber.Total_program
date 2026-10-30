"""챔버12 스텝 실행기 부품 — 기대 토큰(v1 ExpectToken·ExpectGroup 224-297·_send_and_collect_tokens 의 토큰 표 1336-1486),
카운트다운(1633-1708 — v2 일시정지 포함), 실패 정보 정리(_normalize_error_info 1869-1901), 종료 절차 대기값(_shutdown_step_ms 30-51·
rf_power_stop_timeout_ms 157-190), 러너 대기(delay 행·쿨다운 — chamber_runtime.py:5137-5142·5198-5202).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from v2.ch12.process_steps import ProcessStep

HARD_WAIT = ("RF_POWER_STOP", "DC_POWER_STOP", "DC2_POWER_STOP", "RF_PULSE_STOP", "DC_PULSE_STOP")
ZERO_STEP_SLACK_S = 30.0          # process_controller.py:141
# 장치 실패 source → 그 스텝이 만드는 토큰 종류(_device_token_kinds 3033-3051)
SOURCE_TOKENS = {"DCPulse": ("DC_PULSE_TARGET", "DC_PULSE_SET", "DCPULSE_OFF"), "RFPulse": ("RF_TARGET", "RFPULSE_OFF"),
                 "RF Power": ("RF_TARGET", "RF_OFF"), "DC Power": ("DC_TARGET", "DC_OFF"),
                 "DC2 Power": ("DC2_TARGET", "DC2_OFF"), "MFC": ("MFC",), "PLC": ("PLC",), "IG": ("IG_OK",),
                 "RGA": ("RGA_OK",), "HEATER": ("HEATER_SET", "HEATER_READY", "HEATER_HOLD"),
                 "THICKNESS": ("THICKNESS_DONE", "THICKNESS_ZERO")}
_TOKEN_OF = {"DC_POWER_SET": "DC_TARGET", "DC_POWER_STOP": "DC_OFF", "DC2_POWER_SET": "DC2_TARGET",
             "DC2_POWER_STOP": "DC2_OFF", "RF_POWER_SET": "RF_TARGET", "RF_POWER_STOP": "RF_OFF",
             "DC_PULSE_START": "DC_PULSE_TARGET", "DC_PULSE_SET": "DC_PULSE_SET", "DC_PULSE_STOP": "DCPULSE_OFF",
             "RF_PULSE_START": "RF_TARGET", "RF_PULSE_SET": "RF_TARGET", "RF_PULSE_STOP": "RFPULSE_OFF",
             "IG_CMD": "IG_OK", "RGA_SCAN": "RGA_OK", "HEATER_SET": "HEATER_SET", "HEATER_WAIT": "HEATER_READY",
             "HEATER_HOLD": "HEATER_HOLD"}


class StepFailure(Exception):
    """v1 AppError(code, detail) — 스텝 실행 중 콜백 누락 등(E715~E718·E830·E213). 러너 예외 경로로 간다."""

    def __init__(self, code: str, detail: str, meta: Optional[dict] = None):
        super().__init__(f"{code}: {detail}")
        self.code, self.detail, self.meta = code, detail, meta or {}


def tokens_for(step: ProcessStep, norm_plc) -> List[Tuple[str, Any]]:
    """_send_and_collect_tokens 가 만드는 기대 토큰(보내기 전에 계산)."""
    a = step.action
    if a == "PLC_CMD":
        return [("PLC", norm_plc(step.params[0]))]
    if a == "MFC_CMD":
        return [("MFC", step.params[0])]
    t = _TOKEN_OF.get(a)
    return [(t, None)] if t else []


class Expect:
    """ExpectGroup(234-297) — 토큰이 모두 오면 done, cancel 이면 cancelled(사유)."""

    def __init__(self, tokens: List[Tuple[str, Any]]) -> None:
        self.tokens = list(tokens)
        self.done = not self.tokens
        self.cancelled: Optional[str] = None

    def needs(self, kind: str, spec: Any = None) -> bool:
        return any(k == kind and (s is None or s == spec) for k, s in self.tokens)

    def match(self, kind: str, spec: Any = None) -> bool:
        for i, (k, s) in enumerate(self.tokens):
            if k == kind and (s is None or s == spec):
                del self.tokens[i]
                if not self.tokens:
                    self.done = True
                return True
        return False

    def cancel(self, reason: str) -> None:
        if not self.done and self.cancelled is None:
            self.cancelled = reason

    @property
    def over(self) -> bool:
        return self.done or self.cancelled is not None


@dataclass
class Countdown:
    """DELAY 카운트다운 — 일시정지(paused_at) 동안은 시간이 가지 않는다(v2). total 은 ms(v1 _countdown_total_ms)."""
    start: float
    total_ms: int
    msg: str
    paused_at: Optional[float] = None
    paused_s: float = 0.0

    def elapsed_s(self, now: float) -> float:
        p = (now - self.paused_at) if self.paused_at is not None else 0.0
        return max(0.0, now - self.start - self.paused_s - p)

    def remaining_ms(self, now: float) -> int:
        return max(0, self.total_ms - int(self.elapsed_s(now) * 1000.0))


@dataclass
class RunResult:
    ok: bool
    detail: Dict[str, Any] = field(default_factory=dict)


# ───────────── 도우미 ─────────────
def fmt_hms(seconds: float) -> str:
    """_fmt_hms(chamber_runtime.py:3299-3304)."""
    s = int(max(0, seconds))
    return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"


def delay_countdown(clock, total_s: float, show, cancelled, paused=lambda: False) -> bool:
    """러너 대기(delay 행·행 사이 쿨다운): int(초) 번 '표시 → 1 s' (소수 초 버림). cancelled() 면 바로 False.
    paused() 동안은 세지 않는다(v2 일시정지). 끝까지 가면 True."""
    remain = int(total_s)
    while remain > 0:
        show(remain)
        t = clock.monotonic() + 1.0
        if clock.wait_until(t, cancelled):
            return False
        if not paused():
            remain -= 1
    return not cancelled()


def _norm_err(reason: Any, code: Optional[str], meta: Optional[dict]) -> tuple:
    """_normalize_error_info(1869-1901)."""
    c, m = code, {}
    if isinstance(reason, BaseException):
        if c is None:
            c = getattr(reason, "code", None) or getattr(reason, "error_code", None)
        d = getattr(reason, "detail", None) or str(reason)
        if isinstance(getattr(reason, "meta", None), dict):
            m.update(reason.meta)
        m["cause_type"] = type(reason).__name__
    else:
        d = "" if reason is None else str(reason)
    if isinstance(meta, dict):
        m.update(meta)
    d = str(d).strip() or "unknown"
    return c, d, m


def _cfg(cfg, n: int, key: str, default: Any) -> Any:
    v = cfg.get(f"CH{int(n)}_{key}")
    if v is None:
        v = cfg.get(key)
    return default if v is None else v


def _ms(cfg, key: str, default: float) -> float:
    """_shutdown_step_ms(30-43) — 숫자 아님·bool·음수·NaN·inf → 기본값."""
    v = cfg.get(key, default)
    try:
        if isinstance(v, bool):
            return float(default)
        f = float(v)
    except Exception:
        return float(default)
    if f != f or math.isinf(f) or f < 0:
        return float(default)
    return f


def rf_power_stop_timeout_ms(cfg, n: int, base_ms: float) -> float:
    """rf_power_stop_timeout_ms(157-190): max(base, RF_MAX_POWER/RF_RAMP_STEP × 램프다운 간격 + (0 W 확인 + 60) s)."""
    try:
        max_w = float(_cfg(cfg, n, "RF_MAX_POWER", 0.0))
        step_w = float(_cfg(cfg, n, "RF_RAMP_STEP", 0.0))
        iv = float(_cfg(cfg, n, "CHAMBER_RF_CONT_RAMPDOWN_INTERVAL_MS", 0.0))
        zero_s = float(_cfg(cfg, n, "POWER_OFF_ZERO_DEADLINE_S", 10.0))
        if max_w <= 0 or step_w <= 0 or iv <= 0:
            return float(base_ms)
        return float(int(max(base_ms, (max_w / step_w) * iv + (zero_s + 60.0) * 1000.0)))
    except Exception:
        return float(base_ms)

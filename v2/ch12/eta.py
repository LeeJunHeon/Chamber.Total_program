"""챔버12 ETA — 로봇 START_SPUTTER 의 min_total_s 와 GET_SPUTTER_STATUS 의 eta.CHn.remaining_s(사용자 지시 3장 (3)).

v1 근거(기준 커밋 66efc7b — import 하지 않고 다시 씀)
- 행 하한·합(순수 함수): controller/process_controller.py:54-154·193-198(eta_delay_seconds·eta_row_process_time_min·
  eta_row_min_seconds·eta_min_total_seconds) — 런타임 규칙(delay 행 대기·thickness÷dep_rate 역산)과 1:1.
- 확정·남은 초: process_controller.py:1710-1780(_try_confirm_eta·eta_remaining_s) — 'Shutter Delay'(없으면 '메인 공정 진행')
  DELAY 단계에 들어갈 때 1회 확정, 남은 초 = 그 단계부터 모든 DELAY(no_wait 아님) duration 합 + ETA_TAIL_S.
  시퀀스에 THICKNESS_WAIT 가 있으면 확정하지 않는다(remaining_s=null). 종료 절차·정지·실패면 무효(null).
- 하한 보증: process_controller.py:1782-1807(estimate_min_total_s — ETA_TAIL_MIN_S 90, round, 행 없음·예외 → None).
- 로봇 응답: host/handlers.py:477-528 — min_total_s 는 큐(파일 레시피)의 rows[현재 인덱스:] 원본 행으로(단일 시작이면 null),
  remaining_s 는 큐의 마지막 행이 아니면 null, 그 밖에는 eta_remaining_s.
확정 전에는 null — 추정값을 보내지 않는다. 설정 키는 채널 키 CHn_ETA_ENABLED·CHn_ETA_TAIL_S·CHn_ETA_TAIL_MIN_S(params/core.py).
"""
from __future__ import annotations

import re
from typing import Any, Iterable, List, Mapping, Optional

from v2.ch12.recipe import thickness_mode_of

_ETA_DELAY_NAME_RE = re.compile(r"^\s*delay\s*(\d+)\s*([smhd]?)\s*$", re.IGNORECASE)
_ETA_DURATION_RE = re.compile(r"(?:(\d+(?:\.\d+)?)h)?(?:(\d+(?:\.\d+)?)m)?(?:(\d+(?:\.\d+)?)s)?")
CONFIRM_PREFIXES = ("Shutter Delay", "메인 공정 진행")                  # process_controller.py:1717
TAIL_S_DEFAULT = 120.0
TAIL_MIN_S_DEFAULT = 90.0


def _f(v: Any) -> float:
    """빈 문자열/None/이상값 → 0.0(예외 없음)."""
    try:
        txt = str(v).strip() if v is not None else ""
        return float(txt) if txt else 0.0
    except Exception:
        return 0.0


def _parse_duration_seconds(s: str) -> float:
    """chamber_runtime._parse_duration_seconds 와 같은 규칙: '10s'·'1m'·'1h30m' → 초. 단위가 없어 정규식이 못 잡으면 분."""
    if not s:
        return 0.0
    s = s.replace(" ", "").lower()
    m = _ETA_DURATION_RE.match(s)
    if not m:
        try:
            return float(s) * 60.0
        except Exception:
            return 0.0
    return float(m.group(1) or 0) * 3600 + float(m.group(2) or 0) * 60 + float(m.group(3) or 0)


def eta_delay_seconds(row: Optional[Mapping[str, Any]]) -> float:
    """(A) delay 행이면 대기 초(아니면 0). delay 열 숫자만 = 분, 아니면 기간 문자열, 열이 비면 이름 'delay N[smhd]'."""
    row = row or {}
    name = str(row.get("Process_name") or row.get("process_note", "") or "").strip()
    duration_s = 0.0
    dcol = str(row.get("delay", "") or "").strip()
    if dcol:
        try:
            duration_s = float(dcol) * 60.0
        except Exception:
            duration_s = float(_parse_duration_seconds(dcol.lower()))
    else:
        m = _ETA_DELAY_NAME_RE.match(name) if name else None
        if m:
            unit = (m.group(2) or "m").lower()
            duration_s = float(int(m.group(1))) * {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}[unit]
    return duration_s if duration_s > 0 else 0.0


def eta_row_process_time_min(row: Optional[Mapping[str, Any]]) -> float:
    """(B) process_time(분). 비었거나 0 이하이고 dep_rate(또는 dep.rate)>0·thickness 가 있으면 thickness÷dep_rate÷60."""
    row = row or {}
    pt = _f(row.get("process_time", "0"))
    if pt > 0.0:
        return pt
    dep_rate = None
    for key in ("dep_rate", "dep.rate"):
        s = str(row.get(key, "") or "").strip()
        if s:
            try:
                v = float(s)
                dep_rate = v if v > 0 else None
                break
            except Exception:
                pass
    thickness = _f(row.get("thickness", "0")) or None
    if dep_rate and thickness:
        try:
            return float(thickness) / float(dep_rate) / 60.0
        except Exception:
            return pt
    return pt


def eta_row_min_seconds(row: Optional[Mapping[str, Any]], thickness_supported: bool = False) -> float:
    """행 1개의 최소 초 — delay 행 = 대기 초, Thickness 모드 행 = shutter_delay 만, 그 밖 (shutter_delay + process_time)×60."""
    row = row or {}
    d = eta_delay_seconds(row)
    if d > 0:
        return d
    if thickness_mode_of(row.get("thickness"), thickness_supported):
        return _f(row.get("shutter_delay", 0)) * 60.0
    return (_f(row.get("shutter_delay", 0)) + eta_row_process_time_min(row)) * 60.0


def eta_min_total_seconds(rows: Optional[Iterable[Any]], tail_s: float = TAIL_MIN_S_DEFAULT,
                          thickness_supported: bool = False) -> float:
    """Σ 행 하한 + tail_s. 조건 대기(IG/MFC/압력/램프업)는 0."""
    return sum(eta_row_min_seconds(r or {}, thickness_supported) for r in (rows or [])) + float(tail_s)


# ───────────── 설정 ─────────────
def _cv(cfg: Mapping[str, Any], chamber: int, key: str, default: Any) -> Any:
    v = cfg.get(f"CH{int(chamber)}_{key}")
    if v is None:
        v = cfg.get(key)
    return default if v is None else v


def eta_enabled(cfg: Mapping[str, Any], chamber: int) -> bool:
    return bool(_cv(cfg, chamber, "ETA_ENABLED", True))


def tail_s(cfg: Mapping[str, Any], chamber: int) -> float:
    return float(_cv(cfg, chamber, "ETA_TAIL_S", TAIL_S_DEFAULT))


def tail_min_s(cfg: Mapping[str, Any], chamber: int) -> float:
    return float(_cv(cfg, chamber, "ETA_TAIL_MIN_S", TAIL_MIN_S_DEFAULT))


def estimate_min_total_s(rows: Optional[List[Any]], cfg: Mapping[str, Any], chamber: int,
                         thickness_supported: bool = False) -> Optional[int]:
    """estimate_min_total_s(process_controller.py:1782-1807) — ETA_ENABLED 아님·행 없음·예외 → None."""
    try:
        if not eta_enabled(cfg, chamber) or not rows:
            return None
        return max(0, round(eta_min_total_seconds(rows, tail_min_s(cfg, chamber), bool(thickness_supported))))
    except Exception:
        return None


def sum_delay_from(steps: List[Any], idx: int) -> float:
    """idx(포함) 이후 DELAY(no_wait 아님) duration 합(초) — _eta_sum_delay_from."""
    return sum(int(s.duration) for s in steps[idx:] if s.action == "DELAY" and not s.no_wait and s.duration) / 1000.0


class EtaClock:
    """한 공정(컨트롤러 런)의 종료 예정 시각 — 확정 1회·무효화·남은 초(순수 메모리 계산)."""

    def __init__(self, cfg: Mapping[str, Any], chamber: int, monotonic) -> None:
        self.cfg, self.chamber, self.monotonic = cfg, int(chamber), monotonic
        self.end_t: Optional[float] = None

    def reset(self) -> None:
        self.end_t = None

    def try_confirm(self, steps: List[Any], idx: int, *, blocked: bool, log) -> None:
        """_try_confirm_eta(1727-1761) — blocked = 종료 절차·긴급. 로그 '[ETA] 종료 예정 확정: Ns (타이머 Xs + tail Ys)'."""
        if self.end_t is not None or blocked:
            return
        step = steps[idx]
        if step.action != "DELAY":
            return
        if any(s.action == "THICKNESS_WAIT" for s in steps):
            return
        if not str(step.message or "").strip().startswith(CONFIRM_PREFIXES):
            return
        try:
            if not eta_enabled(self.cfg, self.chamber):
                return
            remain = sum_delay_from(steps, idx)
            tail = tail_s(self.cfg, self.chamber)
            total = max(0.0, remain + tail)
            self.end_t = self.monotonic() + total
            log("Process", f"[ETA] 종료 예정 확정: {total:.0f}s (타이머 {remain:.0f}s + tail {tail:.0f}s)")
        except Exception as e:
            self.end_t = None
            log("Process", f"[ETA] 확정 실패(무시): {e}")

    def shift(self, dt: float) -> None:
        """일시정지·남은 시간 수정으로 타이머가 늘거나 줄면 같은 만큼 옮긴다(v2 — v1 에는 공정 중 수정이 없었다)."""
        if self.end_t is not None:
            self.end_t += float(dt)

    def remaining_s(self, *, running: bool, invalid: bool) -> Optional[int]:
        """eta_remaining_s(1763-1780) — invalid = 종료 절차·정지 요청·긴급·실패."""
        try:
            if not eta_enabled(self.cfg, self.chamber) or not running or self.end_t is None or invalid:
                return None
            return max(0, round(self.end_t - self.monotonic()))
        except Exception:
            return None


def host_eta(engines: Mapping[str, Any]) -> dict:
    """GET_SPUTTER_STATUS eta 블록(_build_eta) — {"CH1": {"remaining_s"}, "CH2": {...}}. engines = {"ch1": 엔진, ...}."""
    out = {}
    for n in (1, 2):
        entry: dict = {"remaining_s": None}
        e = engines.get(f"ch{n}")
        try:
            if e is not None:
                entry["remaining_s"] = e.eta_remaining_s()
        except Exception:
            pass
        out[f"CH{n}"] = entry
    return out

"""챔버12 공정 모니터 — 공정 중 가스 유량·작업 압력 편차 알림, DC Pulse Arc·RF Pulse REFP 경고(v1 controller/process_monitor.py
+ runtime/chamber_runtime.py 의 모니터 연결부 433-446·998-1003·1757-1793·2038-2040·2228-2248·2528-2552·2636-2678).

v1 그대로(문구·판정·계기)
- activate(params): use_ms=T 일 때만 목표 등록 — 사용 가스(use_ar/o2/n2 이고 유량>0)와 working_pressure(>0). 새 공정이면 열린 사건을 잊는다.
- Main Shutter 열림 → 감시 시작(목표가 있을 때만), 닫힘 → 끝나지 않은 사건 요약 후 감시 중단. 공정 끝(run_end) → 요약 후 해제.
- 판정: |실측−세팅| ≥ 허용(PROCESS_MONITOR_TOLERANCE_FLOW_SCCM 0.3 sccm, _PRESSURE_MTORR 0.3 mTorr)이면 범위 밖.
  항목(가스별 flow·pressure)마다 '범위 밖 → 돌아옴'을 사건 하나로:
  · 처음 벗어남        : 즉시 '⚠️ CH{n} {Ar flow|Pressure} 편차\\n세팅: x → 실측: y (편차: +d)'(flow 소수 2, 압력 3)
  · 벗어난 채로 계속    : PROCESS_MONITOR_REPEAT_S(300 s, 0 = 없음)마다 '… 편차 계속 (벗어난 지 …)' 요약
  · 범위 안 RECOVER_S(30 s) 이상 : '✅ CH{n} … 정상 복귀 (벗어나 있던 시간 …)' — 그 전에 다시 벗어나면 같은 사건
  · 셔터 닫힘·공정 끝   : 끝나지 않은 사건마다 1건('… — {셔터 닫힐 때|공정 끝날 때}까지 계속' 또는 복귀 중이면 복귀)
- 값의 계기: v1 은 MFC 폴링(MFC_POLLING_INTERVAL_MS 3 s, 메인 공정 폴링 구간 — _last_polling_targets['mfc'])의 flow·pressure
  이벤트마다 check_flow·check_pressure 를 불렀다. v2 MFC 드라이버도 같은 주기·같은 값으로 'flow'·'pressure' 이벤트를 낸다
  (drivers/mfc.py _v1_poll — 1초 읽기 값 중 그 주기에 처음 온 값) — 그 이벤트를 그대로 쓴다(1초 행마다 보면 '범위 밖 N회'가
  v1 의 3배가 된다). 메인 공정 폴링 구간 = 엔진 actions.mfc_polling(v1 _last_polling_targets['mfc']).
- DC Pulse Arc: 드라이버 'arc_threshold_reached' → 공정당 1회(v1 _runtime_arc_notified — 공정 시작 때 초기화)
  '⚠️ CH{n} DC Pulse Arc 경고\\n공정: …\\nSoft Arc: a회  Hard Arc: b회  합계: c회'. 공정 로그의 상세 문구(사유·경과·점화/증착·
  아크율·원시값)는 arc_log_text — process_events 가 같은 공정당 1회 규칙으로 남긴다.
- RF Pulse REFP: status 문구에 'REFP_WARN' 또는 'REFP 정상 복귀' 가 있으면 숫자 W 두 개를 뽑아 즉시
  '⚠️ CH{n} REFP 경고 (공정 계속 진행)\\n공정: …\\nREFP: x.xW  ≥  기준 y.yW' / '✅ CH{n} REFP 정상 복귀 … <'.
- 공정명: 엔진 params 의 process_name(없으면 'CH{n}').
- 설정값은 부를 때마다 cfg 에서 읽는다(v1 config_common 실행 시점 읽기 — 파라미터 apply=live). 잘못된 REPEAT/RECOVER 는 기본값.

v2 방식(전송만 다름)
- v1 은 별도 웹훅(CHAT_WEBHOOK_MONITOR_URL, {'text'} POST 3 s)으로 직접 보냈다. v2 는 이벤트 하나 — system.emit(
  source='process:<ch>', kind='monitor_card', detail=문구, event={chamber, card:'MONITOR', text, item, phase}) → 허브 → 포털이
  MONITOR 웹훅 카드로 보낸다(구글챗은 포털 몫 — 지시 3장 (7)). 엔진 로그('Monitor' 태그 — v1 append_log)는 그대로.
- DEV_MODE 면 v1 처럼 카드는 내지 않고 로그만.

- 엑셀 저장 Arc 경고(v1 gdrive_logger _send_arc_chat — 같은 MONITOR 웹훅, 텍스트 '⚠️ *CH{n} Arc 경고*…')도 monitor_card 로
  (attach 가 engine.excel.on_arc_alert 에 꽂는다 — 엑셀은 DEV_MODE 면 저장 자체를 생략, v1 그대로).

붙이는 법(조립 — 엔진을 만든 뒤): mon = attach(system, engine). system.event_listeners 에 on_event, engine.shutter_listeners 에
  on_shutter, engine.excel.on_arc_alert 에 excel_arc_alert 를 넣는다(엔진은 Main Shutter 명령이 확인될 때 shutter_listeners 를
  부른다). detach() 로 뗀다.
"""
from __future__ import annotations

import math
import re
import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional

_DEFAULT_REPEAT_S = 300.0          # lib/config_common.py:841
_DEFAULT_RECOVER_S = 30.0          # lib/config_common.py:842
_DEFAULT_TOL = 0.3                 # lib/config_common.py:838-839 (runtime/chamber_runtime.py:443-444 getattr 기본값)
CARD = "MONITOR"
KIND = "monitor_card"
_W_RE = re.compile(r"(\d+(?:\.\d+)?)\s*W")


def _seconds_or(v: Any, default: float) -> float:
    """초 단위 값 검사 — 숫자가 아니거나(bool 포함) 음수·무한대·NaN 이면 기본값(process_monitor.py:48-58)."""
    if isinstance(v, bool):
        return default
    try:
        f = float(v)
    except Exception:
        return default
    if not math.isfinite(f) or f < 0:
        return default
    return f


def fmt_dur(sec: float) -> str:
    """'N초' · 'M분 S초' · 'H시간 M분'(process_monitor.py:72-81)."""
    s = max(0, int(round(sec)))
    if s < 60:
        return f"{s}초"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}분 {s}초"
    h, m = divmod(m, 60)
    return f"{h}시간 {m}분"


@dataclass
class _Excursion:
    """한 항목(가스 flow 또는 pressure)의 '범위 밖' 사건 하나."""
    label: str
    unit: str
    nd: int
    target: float
    start: float
    last_report: float
    value: float = 0.0
    diff: float = 0.0
    max_diff: float = 0.0
    out_count: int = 0
    back_since: Optional[float] = None

    def note_out(self, value: float, diff: float) -> None:
        self.value, self.diff = value, diff
        self.out_count += 1
        self.back_since = None
        if abs(diff) > abs(self.max_diff):
            self.max_diff = diff


def arc_log_text(n: int, d: Mapping[str, Any], proc_name: str, soft: int, hard: int) -> str:
    """공정 로그(DCPulse{n})의 Arc 경고 상세 문구(chamber_runtime.py:2649-2672 그대로)."""
    el = float(d.get("elapsed_s", 0.0) or 0.0)
    el_txt = f"{int(el // 60)}분{int(el % 60):02d}초" if el >= 60 else f"{el:.0f}초"
    rate, calc, rate_lim = d.get("rate"), d.get("calc_rate"), d.get("rate_limit")
    why = ", ".join(w for w, on in (("아크율", d.get("by_rate")), ("점화 구간 과다", d.get("by_ign")),
                                    ("증착 구간 과다", d.get("by_depo"))) if on) or "-"
    return (f"⚠️ CH{n} DC Pulse Arc 경고 ({why})\n"
            f"공정: {proc_name}\n"
            f"출력 ON 후 {el_txt} — 런 누적 Soft {soft} / Hard {hard} (합 {soft + hard})\n"
            f"점화 {d.get('ign_total', '-')} (기준 {d.get('ign_limit', '-')}) / "
            f"증착 {d.get('depo_total', '-')} (기준 {d.get('depo_limit', '-')})\n"
            f"최근 아크율 장비 {rate if rate is not None else '-'}/s, "
            f"계산 {calc if calc is not None else '-'}/s "
            f"(기준 {rate_lim}/s x{d.get('rate_n', '')})\n"
            f"장비 원시값 SAT={d.get('raw_soft', '-')} ANT={d.get('raw_hard', '-')}")


def arc_counts(ev: Any) -> tuple:
    """arc_threshold_reached 이벤트 → (soft, hard) (chamber_runtime.py:2643-2644 — data run_soft/run_hard)."""
    d = getattr(ev, "data", None) or {}
    soft = int(d.get("run_soft", getattr(ev, "power", 0) or 0))
    hard = int(d.get("run_hard", getattr(ev, "voltage", 0) or 0))
    return soft, hard


def parse_refp(msg: str) -> Optional[tuple]:
    """RF Pulse status 문구 → (refp_w, warn_w, recovered) 또는 None(REFP 경고·복귀 문구가 아님) — chamber_runtime.py:2531-2546."""
    if "REFP_WARN" not in msg and "REFP 정상 복귀" not in msg:
        return None
    nums = _W_RE.findall(msg)
    refp = float(nums[0]) if len(nums) >= 1 else 0.0
    warn = float(nums[1]) if len(nums) >= 2 else 0.0
    return refp, warn, ("정상 복귀" in msg)


class ProcessMonitor:
    """공정 파라미터 편차 감시(v1 ProcessMonitor 와 같은 API·판정). 알림 한 건 = log_func('Monitor', 문구) + on_card(문구, 정보)."""

    def __init__(self, *, ch: int, cfg: Optional[Mapping[str, Any]] = None,
                 on_card: Optional[Callable[[str, dict], None]] = None,
                 log_func: Optional[Callable[[str, str], None]] = None,
                 flow_tolerance: Optional[float] = None, pressure_tolerance: Optional[float] = None,
                 repeat_s: Optional[float] = None, recover_s: Optional[float] = None,
                 now_func: Optional[Callable[[], float]] = None) -> None:
        import time
        self._ch = int(ch)
        self.cfg = cfg if cfg is not None else {}
        self.on_card = on_card
        self._log_func = log_func
        self._flow_tol_arg, self._pressure_tol_arg = flow_tolerance, pressure_tolerance
        self._repeat_s_arg, self._recover_s_arg = repeat_s, recover_s
        self._now = now_func or time.monotonic
        self._targets: Dict[str, float] = {}
        self._active = False
        self._dev: Dict[str, _Excursion] = {}
        self._lk = threading.RLock()
        self.sent = 0                     # 낸 알림 수(진단·시험)

    # ── 설정값(실행 시점) ──
    def _cfg_num(self, key: str, default: float) -> float:
        try:
            v = self.cfg.get(key, default)
            return default if v is None else float(v)
        except Exception:
            return default

    def _flow_tol(self) -> float:
        return float(self._flow_tol_arg) if self._flow_tol_arg is not None else \
            self._cfg_num("PROCESS_MONITOR_TOLERANCE_FLOW_SCCM", _DEFAULT_TOL)

    def _pressure_tol(self) -> float:
        return float(self._pressure_tol_arg) if self._pressure_tol_arg is not None else \
            self._cfg_num("PROCESS_MONITOR_TOLERANCE_PRESSURE_MTORR", _DEFAULT_TOL)

    def _repeat_s(self) -> float:
        v = self._repeat_s_arg if self._repeat_s_arg is not None else self.cfg.get("PROCESS_MONITOR_REPEAT_S", _DEFAULT_REPEAT_S)
        return _seconds_or(v, _DEFAULT_REPEAT_S)

    def _recover_s(self) -> float:
        v = self._recover_s_arg if self._recover_s_arg is not None else self.cfg.get("PROCESS_MONITOR_RECOVER_S", _DEFAULT_RECOVER_S)
        return _seconds_or(v, _DEFAULT_RECOVER_S)

    # ── 라이프사이클(process_monitor.py:159-197) ──
    def activate(self, params: Mapping[str, Any]) -> None:
        with self._lk:
            self._targets.clear()
            self._dev.clear()
            self._active = False
            if not params.get("use_ms", False):
                return
            for key, gas_name in (("ar", "Ar"), ("o2", "O2"), ("n2", "N2")):
                if params.get(f"use_{key}", False):
                    flow = float(params.get(f"{key}_flow", 0) or 0)
                    if flow > 0:
                        self._targets[gas_name] = flow
            wp = float(params.get("working_pressure", 0) or 0)
            if wp > 0:
                self._targets["pressure"] = wp

    def deactivate(self) -> None:
        with self._lk:
            self._flush("공정 끝날 때")
            self._active = False
            self._targets.clear()

    def notify_shutter_open(self) -> None:
        with self._lk:
            if self._targets:
                self._active = True

    def notify_shutter_close(self) -> None:
        with self._lk:
            self._flush("셔터 닫힐 때")
            self._active = False

    @property
    def is_active(self) -> bool:
        return self._active

    @property
    def targets(self) -> Dict[str, float]:
        return dict(self._targets)

    # ── 체크 ──
    def check_flow(self, gas: str, value: float) -> None:
        with self._lk:
            if not self._active or gas not in self._targets:
                return
            self._observe(gas, f"{gas} flow", "sccm", 2, self._targets[gas], float(value), self._flow_tol())

    def check_pressure(self, value: float) -> None:
        with self._lk:
            if not self._active or "pressure" not in self._targets:
                return
            self._observe("pressure", "Pressure", "mTorr", 3, self._targets["pressure"], float(value), self._pressure_tol())

    def _observe(self, key: str, label: str, unit: str, nd: int, target: float, value: float, tol: float) -> None:
        diff = value - target
        out = abs(diff) >= tol
        now = self._now()
        ex = self._dev.get(key)
        if out:
            if ex is None:
                ex = _Excursion(label=label, unit=unit, nd=nd, target=target, start=now, last_report=now)
                ex.note_out(value, diff)
                self._dev[key] = ex
                self._alert(f"⚠️ CH{self._ch} {label} 편차\n"
                            f"세팅: {target:.{nd}f} {unit} → 실측: {value:.{nd}f} {unit} (편차: {diff:+.{nd}f})",
                            key, "first")
                return
            ex.note_out(value, diff)
            rep = self._repeat_s()
            if rep > 0 and (now - ex.last_report) >= rep:
                ex.last_report = now
                self._alert(f"⚠️ CH{self._ch} {label} 편차 계속 (벗어난 지 {fmt_dur(now - ex.start)})\n"
                            f"{self._detail(ex, last=False)}", key, "repeat")
            return
        if ex is None:
            return
        ex.value, ex.diff = value, diff
        if ex.back_since is None:
            ex.back_since = now
        if (now - ex.back_since) >= self._recover_s():
            self._dev.pop(key, None)
            self._alert(self._recovered_text(ex), key, "recovered")

    def _detail(self, ex: _Excursion, *, last: bool) -> str:
        nd, u = ex.nd, ex.unit
        word = "마지막 실측" if last else "실측"
        return (f"세팅: {ex.target:.{nd}f} {u} → {word}: {ex.value:.{nd}f} {u} (편차: {ex.diff:+.{nd}f})"
                f" · 최대 편차: {ex.max_diff:+.{nd}f} · 범위 밖 {ex.out_count}회")

    def _recovered_text(self, ex: _Excursion) -> str:
        nd, u = ex.nd, ex.unit
        back = ex.back_since if ex.back_since is not None else self._now()
        return (f"✅ CH{self._ch} {ex.label} 정상 복귀 (벗어나 있던 시간 {fmt_dur(back - ex.start)})\n"
                f"세팅: {ex.target:.{nd}f} {u} → 실측: {ex.value:.{nd}f} {u}"
                f" · 최대 편차: {ex.max_diff:+.{nd}f} · 범위 밖 {ex.out_count}회")

    def _flush(self, when: str) -> None:
        if not self._dev:
            return
        items = list(self._dev.items())
        self._dev.clear()
        now = self._now()
        for key, ex in items:
            if ex.back_since is not None:
                self._alert(self._recovered_text(ex), key, "recovered")
            else:
                self._alert(f"⚠️ CH{self._ch} {ex.label} 편차 — {when}까지 계속 (벗어난 지 {fmt_dur(now - ex.start)})\n"
                            f"{self._detail(ex, last=True)}", key, "flush")

    # ── 즉시 경고(process_monitor.py:290-318) ──
    def notify_arc_warning(self, soft_arc: int, hard_arc: int, process_name: str = "") -> None:
        total = soft_arc + hard_arc
        lines = [f"⚠️ CH{self._ch} DC Pulse Arc 경고"]
        if process_name:
            lines.append(f"공정: {process_name}")
        lines.append(f"Soft Arc: {soft_arc}회  Hard Arc: {hard_arc}회  합계: {total}회")
        self._alert("\n".join(lines), "arc", "arc")

    def notify_refp_warning(self, reflected_w: float, warn_w: float, *, recovered: bool = False,
                            process_name: str = "") -> None:
        lines = [f"✅ CH{self._ch} REFP 정상 복귀"] if recovered else [f"⚠️ CH{self._ch} REFP 경고 (공정 계속 진행)"]
        if process_name:
            lines.append(f"공정: {process_name}")
        if recovered:
            lines.append(f"REFP: {reflected_w:.1f}W  <  기준 {warn_w:.1f}W")
        else:
            lines.append(f"REFP: {reflected_w:.1f}W  ≥  기준 {warn_w:.1f}W")
        self._alert("\n".join(lines), "refp", "refp_recovered" if recovered else "refp")

    # ── 알림 한 건 ──
    def _alert(self, message: str, item: str, phase: str) -> None:
        self.sent += 1
        if self._log_func is not None:
            try:
                self._log_func("Monitor", message)
            except Exception:
                pass
        if bool(self.cfg.get("DEV_MODE", False)):          # v1: 개발자 모드면 모니터 웹훅 차단(Monitor 로그는 유지)
            return
        if self.on_card is not None:
            try:
                self.on_card(message, {"item": item, "phase": phase})
            except Exception:
                pass


class EngineMonitor:
    """챔버 엔진 하나에 붙은 공정 모니터(조립이 attach 로 만든다). 내부 이벤트·셔터 확인을 v1 연결부처럼 ProcessMonitor 로 넘긴다."""

    def __init__(self, system, engine, **kw: Any) -> None:
        self.sys, self.engine = system, engine
        self.ch = str(engine.ch)
        self.n = int(engine.n)
        self.src = f"process:{self.ch}"
        self._arc_run: Any = object()       # Arc 경고를 낸 공정(run_id) — 공정당 1회(v1 _runtime_arc_notified)
        self._run: Any = None
        self.mon = ProcessMonitor(ch=self.n, cfg=system.cfg, on_card=self._card, log_func=engine.log,
                                  now_func=system.clock.monotonic, **kw)
        self._sources = {f"engine:{self.ch}", f"{self.ch}.mfc", f"{self.ch}.rfpulse", f"{self.ch}.dcpulse"}

    def _card(self, text: str, info: dict) -> None:
        level = "info" if info.get("phase") in ("recovered", "refp_recovered") else "warn"
        self.sys.emit(self.src, KIND, text, level=level,
                      event={"chamber": self.ch, "card": CARD, "text": text, "item": info.get("item"),
                             "phase": info.get("phase"), "run_id": getattr(self.engine, "run_id", None)})

    def _proc_name(self) -> str:
        p = getattr(self.engine, "params", None) or {}
        return str(p.get("process_name") or f"CH{self.n}")

    def _polling(self) -> bool:
        a = getattr(self.engine, "actions", None)
        return bool(getattr(a, "mfc_polling", False))

    # ── 연결부 ──
    def on_shutter(self, opened: bool) -> None:
        """Main Shutter 열림·닫힘 확인(chamber_runtime.py:998-1003)."""
        if opened:
            self.mon.notify_shutter_open()
        else:
            self.mon.notify_shutter_close()

    def on_event(self, lg) -> None:
        src = lg.source
        if src not in self._sources:
            return
        k, ev = lg.kind, lg.event
        try:
            if src.startswith("engine:"):
                if k == "run_start":
                    d = getattr(ev, "data", None) or {}
                    self._run = d.get("run_id")
                    self.mon.activate(d.get("params") or getattr(self.engine, "params", {}) or {})
                elif k == "run_end":
                    self.mon.deactivate()
            elif src.endswith(".mfc"):
                if k == "flow" and self._polling():
                    self.mon.check_flow(str(getattr(ev, "gas", "") or ""), float(getattr(ev, "value", 0.0) or 0.0))
                elif k == "pressure" and self._polling():
                    v = getattr(ev, "value", None)
                    if v is not None:
                        self.mon.check_pressure(float(v))
            elif src.endswith(".rfpulse"):
                if k == "status":
                    got = parse_refp(str(getattr(ev, "detail", "") or lg.detail or ""))
                    if got is not None:
                        self.mon.notify_refp_warning(got[0], got[1], recovered=got[2], process_name=self._proc_name())
            elif src.endswith(".dcpulse"):
                if k == "arc_threshold_reached":
                    run = getattr(self.engine, "run_id", None)
                    if self._arc_run == run:
                        return
                    self._arc_run = run
                    soft, hard = arc_counts(ev)
                    self.mon.notify_arc_warning(soft_arc=soft, hard_arc=hard, process_name=self._proc_name())
        except Exception as e:                            # v1: contextlib.suppress — 모니터 오류가 공정을 막지 않는다
            try:
                self.engine.log(f"MAIN{self.n}", f"공정 모니터 처리 예외: {e!r} (src={src}, kind={k})")
            except Exception:
                pass

    def excel_arc_alert(self, ch: int, process_name: str, soft: int, hard: int) -> None:
        """엑셀 저장 Arc 경고(util/gdrive_logger.py:467-488 _send_arc_chat — 같은 MONITOR 웹훅, 로그 없음) → monitor_card.
        ExcelLog.on_arc_alert 자리(성공 공정·전 행 최대 soft+hard ≥ GDRIVE_ARC_ALERT_THRESH·런타임 Arc 경고 없었을 때만)."""
        text = (f"⚠️ *CH{int(ch)} Arc 경고*\n"
                f"공정: `{process_name}`\n"
                f"Soft Arc: {soft}회 / Hard Arc: {hard}회 (합계 {soft + hard}회)")
        self._card(text, {"item": "arc", "phase": "excel_arc"})

    def detach(self) -> None:
        for lst, f in ((self.sys.event_listeners, self.on_event),
                       (getattr(self.engine, "shutter_listeners", []), self.on_shutter)):
            while f in lst:
                lst.remove(f)


def attach(system, engine, **kw: Any) -> EngineMonitor:
    """엔진(ProcessEngine) 하나에 공정 모니터를 붙인다 — 조립이 엔진을 만든 뒤 챔버마다 1번."""
    m = EngineMonitor(system, engine, **kw)
    system.event_listeners.append(m.on_event)
    lst = getattr(engine, "shutter_listeners", None)
    if lst is None:
        lst = engine.shutter_listeners = []
    lst.append(m.on_shutter)
    ex = getattr(engine, "excel", None)
    if ex is not None and getattr(ex, "on_arc_alert", None) is None:
        ex.on_arc_alert = m.excel_arc_alert
    engine.monitor = m
    return m

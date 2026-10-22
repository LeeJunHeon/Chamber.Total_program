# controller/process_monitor.py
# -*- coding: utf-8 -*-
"""
공정 중 Gas Flow / Pressure 편차 모니터링 → 별도 Google Chat 웹훅 알림

Main Shutter open 이후 폴링 구간에서 실측값을 세팅값과 비교한다.
항목(가스별 flow, pressure)마다 '범위 밖으로 나감 → 돌아옴' 을 한 사건으로 묶어 알린다.
(예전에는 범위 밖이면 폴링(3초)마다 1건씩 보내 한 공정에 수백 건이 쌓였다.)
  - 처음 벗어난 순간          : 즉시 1건 (예전 문구 그대로)
  - 벗어난 채로 계속          : PROCESS_MONITOR_REPEAT_S 초마다 요약 1건 (0 이면 요약 없음)
  - 범위 안으로 돌아와
    PROCESS_MONITOR_RECOVER_S 초 이상 계속 안에 있음 : ✅ 정상 복귀 1건 (벗어나 있던 시간·최대 편차)
    → 그 전에 다시 벗어나면 같은 사건으로 이어서 센다(새 알림 없음)
  - 셔터가 닫히거나 공정이 끝날 때 아직 끝나지 않은 사건 : 요약 1건
Arc / REFP 경고(notify_arc_warning / notify_refp_warning)는 예전처럼 부를 때마다 즉시 보낸다.
REPEAT_S / RECOVER_S 는 settings.json common 값을 실행 시점에 읽는다(생성자에서 직접 주면 그 값).

사용법 (chamber_runtime.py):
    from controller.process_monitor import ProcessMonitor

    # 초기화 (__init__)
    monitor_url = getattr(cfgl, "CHAT_WEBHOOK_MONITOR_URL", "").strip()
    self._process_monitor = ProcessMonitor(ch=self.ch, webhook_url=monitor_url)

    # 공정 시작 시 (_pump_pc_events, kind=="started")
    self._process_monitor.activate(params)

    # 폴링 데이터 수신 시 (_pump_mfc_events, polling 구간)
    self._process_monitor.check_flow(gas, flow)
    self._process_monitor.check_pressure(value)

    # 공정 종료 시 (_pump_pc_events, kind=="finished")
    self._process_monitor.deactivate()
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import ssl
import time
import urllib.request
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Set

# settings.json common 에 없거나 값이 잘못됐을 때 쓰는 기본값(초)
_DEFAULT_REPEAT_S = 300.0
_DEFAULT_RECOVER_S = 30.0


def _seconds_or(v, default: float) -> float:
    """초 단위 값 검사 — 숫자가 아니거나(bool 포함) 음수·무한대·NaN 이면 기본값."""
    if isinstance(v, bool):
        return default
    try:
        f = float(v)
    except Exception:
        return default
    if not math.isfinite(f) or f < 0:
        return default
    return f


def _cfg_seconds(name: str, default: float) -> float:
    """config_common 의 값을 실행 시점에 읽는다(main.py 의 load_settings 뒤 값)."""
    try:
        from lib import config_common as _cc
        v = getattr(_cc, name, default)
    except Exception:
        return default
    return _seconds_or(v, default)


def _fmt_dur(sec: float) -> str:
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
    label: str                  # "Ar flow" / "Pressure"
    unit: str                   # "sccm" / "mTorr"
    nd: int                     # 표시 소수 자릿수 (flow 2, pressure 3)
    target: float
    start: float                # 처음 벗어난 시각
    last_report: float          # 마지막으로 알린 시각(첫 알림·요약)
    value: float = 0.0          # 마지막 실측
    diff: float = 0.0           # 마지막 편차(실측 - 세팅)
    max_diff: float = 0.0       # 절대값이 가장 컸던 편차(부호 유지)
    out_count: int = 0          # 범위 밖 측정 횟수
    back_since: Optional[float] = None   # 범위 안으로 돌아온 시각(다시 벗어나면 None)

    def note_out(self, value: float, diff: float) -> None:
        self.value, self.diff = value, diff
        self.out_count += 1
        self.back_since = None
        if abs(diff) > abs(self.max_diff):
            self.max_diff = diff


class ProcessMonitor:
    """
    공정 파라미터 편차 감시 + 별도 Google Chat 웹훅 알림.

    - activate(params) 로 세팅값 등록 (사용 중인 가스 + working pressure)
    - check_flow / check_pressure 로 매 폴링마다 비교
    - ±tolerance 를 벗어나면 첫 1건은 즉시, 이후는 사건 단위(요약·복귀·종료 요약)로만 전송
    - notify_shutter_close() / deactivate() 때 끝나지 않은 사건 요약 후 감시 해제
    """

    def __init__(
        self,
        *,
        ch: int,
        webhook_url: str = "",
        flow_tolerance: float = 0.3,
        pressure_tolerance: float = 0.3,
        log_func=None,
        repeat_s: Optional[float] = None,
        recover_s: Optional[float] = None,
        now_func: Optional[Callable[[], float]] = None,
    ):
        self._ch = ch
        self._webhook_url = (webhook_url or "").strip()
        self._flow_tol = flow_tolerance        # 가스 유량 ±sccm
        self._pressure_tol = pressure_tolerance # 작업압 ±mTorr
        self._log_func = log_func
        self._targets: Dict[str, float] = {}   # {"Ar": 20.0, "O2": 5.0, "pressure": 3.000}
        self._active = False
        self._ctx = ssl.create_default_context()
        self._pending: Set[asyncio.Task] = set()
        # 사건 단위 알림 상태 — 키: 가스 이름("Ar"/"O2"/"N2") 또는 "pressure"
        self._dev: Dict[str, _Excursion] = {}
        self._repeat_s_arg = repeat_s           # None 이면 PROCESS_MONITOR_REPEAT_S (실행 시점)
        self._recover_s_arg = recover_s         # None 이면 PROCESS_MONITOR_RECOVER_S (실행 시점)
        self._now = now_func or time.monotonic

    # ── 설정값(실행 시점) ──────────────────────────────────
    def _repeat_s(self) -> float:
        if self._repeat_s_arg is not None:
            return _seconds_or(self._repeat_s_arg, _DEFAULT_REPEAT_S)
        return _cfg_seconds("PROCESS_MONITOR_REPEAT_S", _DEFAULT_REPEAT_S)

    def _recover_s(self) -> float:
        if self._recover_s_arg is not None:
            return _seconds_or(self._recover_s_arg, _DEFAULT_RECOVER_S)
        return _cfg_seconds("PROCESS_MONITOR_RECOVER_S", _DEFAULT_RECOVER_S)

    # ── 라이프사이클 ──────────────────────────────────────
    def activate(self, params: dict) -> None:
        self._targets.clear()
        self._dev.clear()          # 새 공정 — 이전 사건은 잊는다(이미 deactivate 에서 요약됨)
        self._active = False

        # use_ms=False면 등록 없이 즉시 종료
        if not params.get("use_ms", False):
            return

        gas_map = {"ar": "Ar", "o2": "O2", "n2": "N2"}
        for key, gas_name in gas_map.items():
            if params.get(f"use_{key}", False):
                flow = float(params.get(f"{key}_flow", 0))
                if flow > 0:
                    self._targets[gas_name] = flow

        wp = float(params.get("working_pressure", 0))
        if wp > 0:
            self._targets["pressure"] = wp

    def deactivate(self) -> None:
        """공정 종료 시 — 끝나지 않은 사건 요약 후 감시 해제."""
        self._flush("공정 끝날 때")
        self._active = False
        self._targets.clear()

    def notify_shutter_open(self) -> None:
        """Main Shutter 열릴 때 감시 시작."""
        if self._targets:
            self._active = True

    def notify_shutter_close(self) -> None:
        """Main Shutter 닫힐 때 — 끝나지 않은 사건 요약 후 감시 중단."""
        self._flush("셔터 닫힐 때")
        self._active = False

    @property
    def is_active(self) -> bool:
        return self._active

    # ── 체크 ─────────────────────────────────────────────

    def check_flow(self, gas: str, value: float) -> None:
        """
        폴링된 가스 유량을 세팅값과 비교.
        ±tolerance 를 벗어나면 사건 단위로 웹훅 알림.
        """
        if not self._active:
            return
        if gas not in self._targets:
            return
        self._observe(gas, f"{gas} flow", "sccm", 2, self._targets[gas], value, self._flow_tol)

    def check_pressure(self, value: float) -> None:
        """
        폴링된 챔버 압력을 세팅값과 비교.
        ±tolerance 를 벗어나면 사건 단위로 웹훅 알림.
        """
        if not self._active:
            return
        if "pressure" not in self._targets:
            return
        self._observe("pressure", "Pressure", "mTorr", 3, self._targets["pressure"], value, self._pressure_tol)

    def _observe(self, key: str, label: str, unit: str, nd: int,
                 target: float, value: float, tol: float) -> None:
        diff = value - target
        out = abs(diff) >= tol
        now = self._now()
        ex = self._dev.get(key)

        if out:
            if ex is None:
                # 처음 벗어남 → 즉시 1건 (예전 문구 그대로)
                ex = _Excursion(label=label, unit=unit, nd=nd, target=target, start=now, last_report=now)
                ex.note_out(value, diff)
                self._dev[key] = ex
                self._alert(
                    f"⚠️ CH{self._ch} {label} 편차\n"
                    f"세팅: {target:.{nd}f} {unit} → 실측: {value:.{nd}f} {unit} "
                    f"(편차: {diff:+.{nd}f})"
                )
                return
            ex.note_out(value, diff)
            rep = self._repeat_s()
            if rep > 0 and (now - ex.last_report) >= rep:
                ex.last_report = now
                self._alert(
                    f"⚠️ CH{self._ch} {label} 편차 계속 (벗어난 지 {_fmt_dur(now - ex.start)})\n"
                    f"{self._detail(ex, last=False)}"
                )
            return

        if ex is None:
            return
        ex.value, ex.diff = value, diff
        if ex.back_since is None:
            ex.back_since = now
        if (now - ex.back_since) >= self._recover_s():
            self._dev.pop(key, None)
            self._alert(self._recovered_text(ex))

    def _detail(self, ex: _Excursion, *, last: bool) -> str:
        nd, u = ex.nd, ex.unit
        word = "마지막 실측" if last else "실측"
        return (f"세팅: {ex.target:.{nd}f} {u} → {word}: {ex.value:.{nd}f} {u} (편차: {ex.diff:+.{nd}f})"
                f" · 최대 편차: {ex.max_diff:+.{nd}f} · 범위 밖 {ex.out_count}회")

    def _recovered_text(self, ex: _Excursion) -> str:
        nd, u = ex.nd, ex.unit
        back = ex.back_since if ex.back_since is not None else self._now()
        return (f"✅ CH{self._ch} {ex.label} 정상 복귀 (벗어나 있던 시간 {_fmt_dur(back - ex.start)})\n"
                f"세팅: {ex.target:.{nd}f} {u} → 실측: {ex.value:.{nd}f} {u}"
                f" · 최대 편차: {ex.max_diff:+.{nd}f} · 범위 밖 {ex.out_count}회")

    def _flush(self, when: str) -> None:
        """셔터 닫힘·공정 종료 — 끝나지 않은 사건마다 1건(돌아와 있으면 복귀, 아니면 '…까지 계속')."""
        if not self._dev:
            return
        items = list(self._dev.values())
        self._dev.clear()
        now = self._now()
        for ex in items:
            if ex.back_since is not None:
                self._alert(self._recovered_text(ex))
            else:
                self._alert(
                    f"⚠️ CH{self._ch} {ex.label} 편차 — {when}까지 계속 (벗어난 지 {_fmt_dur(now - ex.start)})\n"
                    f"{self._detail(ex, last=True)}"
                )

    def notify_arc_warning(self, soft_arc: int, hard_arc: int, process_name: str = "") -> None:
        """Arc 임계값 도달 시 즉시 Monitor 웹훅으로 텍스트 전송."""
        total = soft_arc + hard_arc
        lines = [f"⚠️ CH{self._ch} DC Pulse Arc 경고"]
        if process_name:
            lines.append(f"공정: {process_name}")
        lines.append(f"Soft Arc: {soft_arc}회  Hard Arc: {hard_arc}회  합계: {total}회")
        self._alert("\n".join(lines))

    def notify_refp_warning(
        self,
        reflected_w: float,
        warn_w: float,
        *,
        recovered: bool = False,
        process_name: str = "",
    ) -> None:
        """RF Pulse REFP 경고/정상 복귀를 Monitor 웹훅으로 즉시 전송."""
        if recovered:
            lines = [f"✅ CH{self._ch} REFP 정상 복귀"]
        else:
            lines = [f"⚠️ CH{self._ch} REFP 경고 (공정 계속 진행)"]
        if process_name:
            lines.append(f"공정: {process_name}")
        if recovered:
            lines.append(f"REFP: {reflected_w:.1f}W  <  기준 {warn_w:.1f}W")
        else:
            lines.append(f"REFP: {reflected_w:.1f}W  ≥  기준 {warn_w:.1f}W")
        self._alert("\n".join(lines))

    # ── 웹훅 전송 ────────────────────────────────────────

    def _alert(self, message: str) -> None:
        if self._log_func:
            try:
                self._log_func("Monitor", message)
            except Exception:
                pass

        # ✅ 개발자 모드: 모니터 웹훅 차단 (Monitor 로그는 유지)
        try:
            from lib import config_common as _cc
            if getattr(_cc, "DEV_MODE", False):
                return
        except Exception:
            pass

        if not self._webhook_url:
            return

        payload = {"text": message}
        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(self._post(payload))
            self._pending.add(task)
            task.add_done_callback(lambda t: self._pending.discard(t))
        except RuntimeError:
            pass

    async def _post(self, payload: dict) -> None:
        url = self._webhook_url
        data = json.dumps(payload).encode("utf-8")
        ctx = self._ctx

        def _blocking() -> None:
            req = urllib.request.Request(
                url, data=data,
                headers={"Content-Type": "application/json"},
            )
            with contextlib.suppress(Exception):
                with urllib.request.urlopen(req, timeout=3, context=ctx) as resp:
                    resp.read()

        await asyncio.to_thread(_blocking)

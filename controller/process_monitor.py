# controller/process_monitor.py
# -*- coding: utf-8 -*-
"""
공정 중 Gas Flow / Pressure 편차 모니터링 → 별도 Google Chat 웹훅 알림

Main Shutter open 이후 폴링 구간에서 실측값을 세팅값과 비교하여
±tolerance 이상 벗어나면 즉시 별도 웹훅으로 알림을 전송한다.

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
import ssl
import urllib.request
from typing import Dict, Optional, Set


class ProcessMonitor:
    """
    공정 파라미터 편차 감시 + 별도 Google Chat 웹훅 알림.

    - activate(params) 로 세팅값 등록 (사용 중인 가스 + working pressure)
    - check_flow / check_pressure 로 매 폴링마다 비교
    - ±tolerance 초과 시 즉시 웹훅 전송 (쿨다운 없음)
    - deactivate() 로 감시 해제
    """

    def __init__(
        self,
        *,
        ch: int,
        webhook_url: str = "",
        flow_tolerance: float = 0.3,
        pressure_tolerance: float = 0.3,
        log_func=None,
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

    # ── 라이프사이클 ──────────────────────────────────────
    def activate(self, params: dict) -> None:
        self._targets.clear()
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
        """공정 종료 시 감시 해제."""
        self._active = False
        self._targets.clear()

    def notify_shutter_open(self) -> None:
        """Main Shutter 열릴 때 감시 시작."""
        if self._targets:
            self._active = True

    def notify_shutter_close(self) -> None:
        """Main Shutter 닫힐 때 감시 중단."""
        self._active = False

    @property
    def is_active(self) -> bool:
        return self._active

    # ── 체크 ─────────────────────────────────────────────

    def check_flow(self, gas: str, value: float) -> None:
        """
        폴링된 가스 유량을 세팅값과 비교.
        ±tolerance 초과 시 웹훅 알림.
        """
        if not self._active:
            return
        if gas not in self._targets:
            return

        target = self._targets[gas]
        diff = value - target
        if abs(diff) >= self._flow_tol:
            self._alert(
                f"⚠️ CH{self._ch} {gas} flow 편차\n"
                f"세팅: {target:.2f} sccm → 실측: {value:.2f} sccm "
                f"(편차: {diff:+.2f})"
            )

    def check_pressure(self, value: float) -> None:
        """
        폴링된 챔버 압력을 세팅값과 비교.
        ±tolerance 초과 시 웹훅 알림.
        """
        if not self._active:
            return
        if "pressure" not in self._targets:
            return

        target = self._targets["pressure"]
        diff = value - target
        if abs(diff) >= self._pressure_tol:
            self._alert(
                f"⚠️ CH{self._ch} Pressure 편차\n"
                f"세팅: {target:.3f} mTorr → 실측: {value:.3f} mTorr "
                f"(편차: {diff:+.3f})"
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
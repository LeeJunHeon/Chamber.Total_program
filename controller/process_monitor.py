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
        tolerance: float = 0.3,
    ):
        self._ch = ch
        self._webhook_url = (webhook_url or "").strip()
        self._tolerance = tolerance       # ±0.3 (sccm / mTorr 동일)
        self._targets: Dict[str, float] = {}   # {"Ar": 20.0, "O2": 5.0, "pressure": 3.000}
        self._active = False
        self._ctx = ssl.create_default_context()
        self._pending: Set[asyncio.Task] = set()

    # ── 라이프사이클 ──────────────────────────────────────

    def activate(self, params: dict) -> None:
        """
        공정 시작 시 세팅값을 등록한다.
        사용 중인 가스(use_ar/o2/n2 == True)와 working_pressure만 감시 대상.
        """
        self._targets.clear()

        gas_map = {"ar": "Ar", "o2": "O2", "n2": "N2"}
        for key, gas_name in gas_map.items():
            if params.get(f"use_{key}", False):
                flow = float(params.get(f"{key}_flow", 0))
                if flow > 0:
                    self._targets[gas_name] = flow

        wp = float(params.get("working_pressure", 0))
        if wp > 0:
            self._targets["pressure"] = wp

        # use_ms=False면 targets도 등록하지 않음
        if not params.get("use_ms", False):
            self._targets.clear()
            return

        self._active = False  # shutter open 전까지는 항상 비활성

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
        if abs(diff) >= self._tolerance:
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
        if abs(diff) >= self._tolerance:
            self._alert(
                f"⚠️ CH{self._ch} Pressure 편차\n"
                f"세팅: {target:.3f} mTorr → 실측: {value:.3f} mTorr "
                f"(편차: {diff:+.3f})"
            )

    # ── 웹훅 전송 ────────────────────────────────────────

    def _alert(self, message: str) -> None:
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
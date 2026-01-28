# device/rga.py
# -*- coding: utf-8 -*-
"""
RGA Client (main process side)
- ✅ 메인 프로세스에서는 srsinst/srsgui를 import하지 않는다.
- RGA 측정은 별도 프로세스(rga_worker.exe 또는 rga_worker.py)가 수행한다.
- IPC는 "stdout 1줄 JSON" 방식 (네트워크 통신 아님).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional, Sequence, Union

import numpy as np


@dataclass
class RGAEvent:
    kind: str
    payload: Optional[Dict[str, Any]] = None

    # payload에 있는 키들을 ev.xxx 형태로 접근 가능하게
    def __getattr__(self, name: str):
        if self.payload and name in self.payload:
            return self.payload[name]
        raise AttributeError(name)

    @property
    def message(self) -> Optional[str]:
        if not self.payload:
            return None
        return self.payload.get("message") or self.payload.get("error")

    @property
    def success(self) -> bool:
        if not self.payload:
            return False
        return bool(self.payload.get("success", False))


class RGAWorkerClient:
    def __init__(
        self,
        ch: int,
        worker_path: Optional[Union[str, Path]] = None,
        default_timeout_s: float = 30.0,
        logger: Any = None,
    ):
        self.ch = int(ch)
        self._q: asyncio.Queue[RGAEvent] = asyncio.Queue()
        self._connected = True
        self._default_timeout_s = float(default_timeout_s)
        self._logger = logger
        self._worker_path = Path(worker_path) if worker_path else self._resolve_default_worker_path()

        if self._logger:
            self._logger.info("RGAWorkerClient ready: ch=%s worker=%s", self.ch, self._worker_path)

    @property
    def connected(self) -> bool:
        return self._connected

    async def events(self) -> AsyncIterator[RGAEvent]:
        while self._connected or (not self._q.empty()):
            ev = await self._q.get()
            yield ev

    async def cleanup(self) -> None:
        self._connected = False

    def _log(self, level: str, msg: str, *args) -> None:
        if not self._logger:
            return
        try:
            getattr(self._logger, level)(msg, *args)
        except Exception:
            pass

    def _resolve_default_worker_path(self) -> Path:
        """
        우선순위:
        1) 환경변수 VANAM_RGA_WORKER
        2) PyInstaller frozen: main.exe 옆 rga_worker.exe
        3) 소스 실행: 프로젝트 tools/rga_worker.py
        """
        env = os.getenv("VANAM_RGA_WORKER")
        if env:
            return Path(env)

        if getattr(sys, "frozen", False):
            base = Path(sys.executable).resolve().parent
            for c in (base / "rga_worker.exe", base / "tools" / "rga_worker.exe"):
                if c.exists():
                    return c
            return base / "rga_worker.exe"

        proj_root = Path(__file__).resolve().parent.parent
        return proj_root / "tools" / "rga_worker.py"

    def _build_cmd(self, timeout_s: float) -> Sequence[str]:
        wp = self._worker_path
        if wp.suffix.lower() == ".py":
            return [sys.executable, str(wp), "--ch", str(self.ch), "--timeout", str(timeout_s)]
        return [str(wp), "--ch", str(self.ch), "--timeout", str(timeout_s)]

    def _parse_json_from_stdout(self, out_text: str) -> Optional[Dict[str, Any]]:
        if not out_text:
            return None
        for line in reversed(out_text.splitlines()):
            s = line.strip()
            if s.startswith("{") and s.endswith("}"):
                try:
                    return json.loads(s)
                except Exception:
                    pass
        return None

    async def scan_histogram_to_csv(self, _csv_path: Optional[str] = None, timeout_s: Optional[float] = None) -> None:
        """
        기존 호출부 호환용으로 csv_path 인자는 받지만,
        worker가 자체 CH_CONFIG의 csv 경로를 쓰므로 무시한다.
        """
        if timeout_s is None:
            timeout_s = self._default_timeout_s
        timeout_s = float(timeout_s)

        cmd = self._build_cmd(timeout_s)
        self._log("info", "RGA worker start: %s", " ".join(map(str, cmd)))
        await self._q.put(RGAEvent("status", {"message": f"RGA scan start (ch={self.ch})"}))

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            try:
                out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
            except asyncio.TimeoutError:
                with contextlib.suppress(Exception):
                    proc.kill()
                await self._q.put(RGAEvent("failed", {
                    "success": False,
                    "message": f"timeout {timeout_s}s (worker killed)",
                    "error": f"timeout {timeout_s}s (worker killed)",
                }))
                return

            payload = self._parse_json_from_stdout(out_b)
            if not payload:
                await self._q.put(RGAEvent("failed", {
                    "success": False,
                    "message": "worker stdout JSON not found",
                    "error": "worker stdout JSON not found",
                    "stdout": out_b[-2000:],
                    "stderr": err_b[-2000:],
                }))
                return

            if payload.get("ok"):
                mass_axis = np.asarray(payload.get("mass_axis", []), dtype=float)
                pressures = np.asarray(payload.get("pressures", []), dtype=float)

                await self._q.put(RGAEvent("data", {
                    "mass_axis": mass_axis,
                    "pressures": pressures,
                    "meta": payload,
                }))
                await self._q.put(RGAEvent("finished", {
                    "success": True,
                    "message": "scan finished",
                    "meta": payload,
                }))
            else:
                msg = payload.get("error") or payload.get("message") or "worker returned ok=false"
                await self._q.put(RGAEvent("failed", {
                    "success": False,
                    "message": msg,
                    "meta": payload,
                    "stdout": out_b[-2000:],
                    "stderr": err_b[-2000:],
                }))

        except Exception as e:
            await self._q.put(RGAEvent("failed", {"error": f"{type(e).__name__}: {e}", "traceback": traceback.format_exc(limit=50)}))

# device/rga.py
# -*- coding: utf-8 -*-
"""
RGA Worker Client (main process)
- 메인 프로세스에서는 srsinst를 절대 import 하지 않는다.
- subprocess로 tools/rga_worker.py(또는 rga_worker.exe)를 실행하고
  stdout JSON을 읽어 그래프용 데이터(mass_axis/pressures)를 전달한다.
"""

from __future__ import annotations

import sys
import json
import asyncio
import subprocess
import contextlib  # 파일 상단에 추가
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional, Tuple


@dataclass
class RGAEvent:
    kind: str
    payload: Dict[str, Any]

    def __getattr__(self, name: str) -> Any:
        # ev.mass_axis, ev.pressures, ev.message 같은 접근을 허용
        try:
            return self.payload[name]
        except KeyError as e:
            raise AttributeError(name) from e

    @property
    def message(self) -> str:
        # status/failed 메시지 통일
        return (
            str(self.payload.get("message") or "")
            or str(self.payload.get("error") or "")
            or str(self.payload.get("reason") or "")
        )


class RGAWorkerClient:
    def __init__(
        self,
        ch: int,
        worker_path: Optional[Path] = None,
        logger=None,
        default_timeout_s: float = 30.0,
    ):
        self.ch = int(ch)
        self.worker_path = worker_path
        self.logger = logger
        self.default_timeout_s = float(default_timeout_s)

        self._q: asyncio.Queue[RGAEvent] = asyncio.Queue()
        self._connected = True  # chamber_runtime에서 “있으면 pump” 정도로만 사용

    def _resolve_worker_cmd(self) -> Tuple[str, ...]:
        """
        - 개발 실행: [python, tools/rga_worker.py]
        - 배포 실행: [rga_worker.exe] (main exe와 같은 폴더 또는 tools 폴더)
        """
        # 1) Frozen(=exe) 환경이면 exe를 먼저 찾는다
        if getattr(sys, "frozen", False):
            exe_dir = Path(sys.executable).resolve().parent
            candidates = [
                exe_dir / "rga_worker.exe",
                exe_dir / "tools" / "rga_worker.exe",
                Path.cwd() / "tools" / "rga_worker.exe",  # ✅ 바로가기/작업폴더 꼬임 대비
            ]
            for c in candidates:
                if c.exists():
                    return (str(c),)
                
            checked = " | ".join(str(p) for p in candidates)
            raise FileNotFoundError(f"rga_worker.exe not found. checked: {checked}")

        # 2) 비-frozen이면 python으로 스크립트 실행
        root = Path(__file__).resolve().parent.parent

        if self.worker_path is not None:
            script = Path(self.worker_path)
        else:
            candidates = [
                root / "tools" / "rga_worker.py",                 # 혹시 로컬에 존재하면 사용
                root / "apps" / "rga_service" / "rga_api.py",     # ✅ 현재 프로젝트/스펙 기준
            ]
            script = None
            for c in candidates:
                if c.exists():
                    script = c
                    break
            if script is None:
                checked = " | ".join(str(p) for p in candidates)
                raise FileNotFoundError(f"rga worker script not found. checked: {checked}")

        return (sys.executable, str(script))

    @staticmethod
    def _parse_json_from_stdout(stdout_text: str) -> Optional[Dict[str, Any]]:
        # stdout에 다른 로그가 섞여도 첫 JSON 라인을 찾아서 파싱
        for line in stdout_text.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    return json.loads(line)
                except Exception:
                    continue
        return None

    async def scan_histogram_to_csv(self, timeout_s: Optional[float] = None) -> None:
        """
        worker 실행 → JSON 받기 → 이벤트 푸시
        (csv 저장은 worker가 책임)
        """
        if timeout_s is None:
            timeout_s = self.default_timeout_s

        try:
            cmd_base = self._resolve_worker_cmd()
        except Exception as e:
            await self._q.put(RGAEvent("failed", {"message": f"RGA worker resolve failed: {e!r}"}))
            await self._q.put(RGAEvent("finished", {"message": "RGA scan finished (resolve fail)"}))
            return

        cmd = [*cmd_base, "--ch", str(self.ch), "--timeout", str(timeout_s)]
        await self._q.put(RGAEvent("status", {"message": f"RGA worker start: {' '.join(cmd)}"}))

        creationflags = 0
        if sys.platform.startswith("win"):
            creationflags = subprocess.CREATE_NO_WINDOW

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.DEVNULL,
                creationflags=creationflags,
            )
        except FileNotFoundError as e:
            await self._q.put(RGAEvent("failed", {"message": f"RGA worker exec failed: {e!r}"}))
            await self._q.put(RGAEvent("finished", {"message": "RGA scan finished (exec fail)"}))
            return
        except Exception as e:
            await self._q.put(RGAEvent("failed", {"message": f"RGA worker exec unexpected error: {e!r}"}))
            await self._q.put(RGAEvent("finished", {"message": "RGA scan finished (exec error)"}))
            return

        try:
            out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)

        except asyncio.TimeoutError:
            # 1) 강제 종료 시도
            with contextlib.suppress(Exception):
                proc.kill()

            # 2) OS가 프로세스를 회수하도록 짧게 기다림(윈도우에서 특히 중요)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(proc.wait(), timeout=2.0)

            await self._q.put(RGAEvent("failed", {"message": f"RGA worker timeout ({timeout_s}s)"}))
            await self._q.put(RGAEvent("finished", {"message": "RGA scan finished (timeout)"}))
            return

        out = (out_b or b"").decode("utf-8", errors="replace")
        err = (err_b or b"").decode("utf-8", errors="replace")

        payload = self._parse_json_from_stdout(out)

        if not payload or not isinstance(payload, dict):
            msg = "RGA worker stdout JSON parse failed"
            if self.logger:
                self.logger.error("%s | stderr=%s | stdout=%s", msg, err.strip(), out.strip())
            await self._q.put(RGAEvent("failed", {"message": msg, "stdout": out, "stderr": err}))
            await self._q.put(RGAEvent("finished", {"message": "RGA scan finished (parse fail)"}))
            return

        if not payload.get("ok", False):
            msg = payload.get("error") or payload.get("message") or "RGA worker failed"
            await self._q.put(RGAEvent("failed", {"message": msg, "meta": payload}))
            await self._q.put(RGAEvent("finished", {"message": "RGA scan finished (failed)"}))
            return

        # 성공: 그래프용 데이터
        mass_axis = payload.get("mass_axis") or []
        pressures = payload.get("pressures") or []

        await self._q.put(RGAEvent("data", {"mass_axis": mass_axis, "pressures": pressures, "meta": payload}))
        await self._q.put(RGAEvent("finished", {"message": "RGA scan finished"}))

    async def events(self) -> AsyncIterator[RGAEvent]:
        # ✅ cleanup 때문에 영구 종료되면 다음 공정에서 RGA finished 신호가 안 올라와서 무한대기함
        while True:
            ev = await self._q.get()
            yield ev

    async def cleanup(self) -> None:
        # ✅ 여기서 스트림을 닫지 않는다(외부 pump task cancel로 종료)
        return

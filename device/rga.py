# device/rga.py
# -*- coding: utf-8 -*-
"""
RGA Client (메인 프로세스 안전용)
- 더 이상 srsinst/srsgui를 메인 프로세스에서 import/실행하지 않는다.
- 대신 rga_worker.exe(or rga_worker.py)를 subprocess로 실행하여:
  1) CSV append는 worker가 수행
  2) 측정 결과는 stdout JSON 1줄로 받아서 이벤트로 전달
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Literal

import numpy as np


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class RGAEvent:
    kind: Literal["status", "data", "finished", "failed"]
    code: str = ""
    message: str = ""
    timestamp: str = ""
    mass_axis: Optional[np.ndarray] = None
    pressures: Optional[np.ndarray] = None
    # histogram_raw: Optional[np.ndarray] = None  # 필요해지면 worker에서 보내고 여기서 받으면 됨


class RGA100AsyncAdapter:
    """
    (기존 chamber_runtime.py 호환 목적)
    - 기존처럼 (host, user, password, name)을 받아도 되고
    - scan_histogram_to_csv()를 호출하면 내부적으로 rga_worker를 실행한다.
    """

    def __init__(self, host: str, user: str = "admin", password: str = "admin", name: str = "RGA"):
        self.host = host
        self.user = user
        self.password = password
        self.name = name
        self._events: asyncio.Queue[RGAEvent] = asyncio.Queue(maxsize=200)

    def _ev_nowait(self, ev: RGAEvent) -> None:
        try:
            self._events.put_nowait(ev)
        except Exception:
            # 이벤트 폭주 시 드롭(메인을 죽이면 안 됨)
            pass

    async def events(self):
        """기존 코드가 async for로 소비하는 패턴 대비"""
        while True:
            ev = await self._events.get()
            yield ev
            if ev.kind in ("finished", "failed"):
                break

    async def _status(self, msg: str) -> None:
        self._ev_nowait(RGAEvent(kind="status", message=msg, timestamp=_now_str()))

    async def _finished(self) -> None:
        self._ev_nowait(RGAEvent(kind="finished", message="done", timestamp=_now_str()))

    async def _failed(self, code: str, msg: str) -> None:
        self._ev_nowait(RGAEvent(kind="failed", code=code, message=msg, timestamp=_now_str()))

    def _resolve_worker_cmd(self) -> list[str]:
        """
        실행 우선순위:
        1) 메인 exe 옆 rga_worker.exe
        2) 소스 트리 tools/rga_worker.py 를 python -u로 실행(개발환경)
        """
        # PyInstaller/실운영 exe
        base = Path(sys.executable).resolve().parent
        exe1 = base / "rga_worker.exe"
        exe2 = base / "tools" / "rga_worker.exe"
        if exe1.exists():
            return [str(exe1)]
        if exe2.exists():
            return [str(exe2)]

        # 개발환경: tools/rga_worker.py
        here = Path(__file__).resolve()
        py = here.parent.parent / "tools" / "rga_worker.py"
        if py.exists():
            return [sys.executable, "-u", str(py)]

        # 최후: 현재 작업폴더 기준
        py2 = Path.cwd() / "tools" / "rga_worker.py"
        return [sys.executable, "-u", str(py2)]

    async def scan_histogram_to_csv(self, csv_path: str | Path, *, suffix: str = "e-12", timeout_s: float = 30.0, ch: Optional[int] = None) -> None:
        """
        ✅ 기존 호출부 호환을 위해 시그니처 유지
        - 실제 측정/CSV 저장은 worker가 수행
        - csv_path/suffix/host/user/password는 env로 worker에 전달(커맨드는 --ch/--timeout만)
        - ch는 호출부에서 안 주면 name에서 추출 시도(예: "CH1","CH2")
        """
        await self._status(f"[{self.name}] RGA worker 실행 시작")

        # ch 결정: 외부에서 안 주면 name에서 추론
        ch_val = ch
        if ch_val is None:
            n = (self.name or "").upper()
            if "CH1" in n:
                ch_val = 1
            elif "CH2" in n:
                ch_val = 2
            else:
                # 최소 안전값
                ch_val = 1

        cmd = self._resolve_worker_cmd() + ["--ch", str(ch_val), "--timeout", str(float(timeout_s))]

        env = os.environ.copy()
        # ✅ “파라미터 2개만” 유지하면서도, 실제 값은 env로 정확히 전달 가능
        env["VANAM_RGA_IP"] = str(self.host)
        env["VANAM_RGA_USER"] = str(self.user)
        env["VANAM_RGA_PASSWORD"] = str(self.password)
        env["VANAM_RGA_CSV"] = str(csv_path)
        env["VANAM_RGA_SUFFIX"] = str(suffix)

        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )

            # ✅ 진짜 timeout은 여기서 강제(kill)로 보장
            out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=float(timeout_s) + 5.0)

            out = (out_b or b"").decode("utf-8", errors="replace").strip()
            err = (err_b or b"").decode("utf-8", errors="replace").strip()

            if not out:
                await self._failed("RGA_WORKER_NO_OUTPUT", f"worker produced no stdout. stderr={err[:400]}")
                return

            # stdout 마지막 줄 JSON 파싱(중간 로그가 있어도 마지막 JSON만 믿게)
            last_line = out.splitlines()[-1].strip()
            payload = json.loads(last_line)

            if not payload.get("ok"):
                await self._failed(payload.get("stage", "RGA_WORKER_FAIL"), payload.get("error", "unknown error"))
                return

            mass_axis = np.asarray(payload.get("mass_axis", []), dtype=float)
            pressures = np.asarray(payload.get("pressures", []), dtype=float)

            self._ev_nowait(RGAEvent(
                kind="data",
                timestamp=payload.get("timestamp", _now_str()),
                mass_axis=mass_axis,
                pressures=pressures,
                message=f"csv={payload.get('csv_path','')}",
            ))

            await self._status(f"[{self.name}] worker 완료 ({payload.get('duration_ms')} ms)")
            await self._finished()

        except asyncio.TimeoutError:
            if proc and proc.returncode is None:
                try:
                    proc.kill()
                except Exception:
                    pass
            await self._failed("RGA_WORKER_TIMEOUT", f"timeout_s={timeout_s}")

        except Exception as e:
            await self._failed("RGA_WORKER_EXCEPTION", f"{type(e).__name__}: {e}")

        finally:
            # 혹시 남아있으면 정리
            try:
                if proc and proc.returncode is None:
                    proc.kill()
            except Exception:
                pass

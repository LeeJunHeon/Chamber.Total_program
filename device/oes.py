# device/oes.py
# -*- coding: utf-8 -*-
"""
device/oes.py (메인 프로그램용 OES "클라이언트")

목표(요구사항):
- OES 실측/DLL 호출은 외부 워커( apps/oes_service/oes_api.exe )에서만 수행
- 메인 프로그램은
  1) 워커 exe 실행(init/measure)
  2) 워커가 로컬에 append+flush 하는 CSV를 tail 해서 실시간 그래프 이벤트(data) 발행
  3) 측정 완료 후 NAS로 "이동"(copy+검증+로컬삭제). 이동 실패 시 로컬 유지
- 기존 chamber_runtime.py 의 호출부( from device.oes import OESAsync )는 유지
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator, Literal, Optional, List

import numpy as np

# ====== 고정 경로(사용자 요구) ======
_LOCAL_BASE = Path(r"C:\Users\vanam\Desktop\oes")
_NAS_BASE_CH1 = Path(r"\\VanaM_NAS\VanaM_Sputter\OES\CH1")
_NAS_BASE_CH2 = Path(r"\\VanaM_NAS\VanaM_Sputter\OES\CH2")
# ====================================

EventKind = Literal["status", "data", "finished"]


@dataclass
class OESEvent:
    kind: EventKind
    message: str = ""
    x: Optional[np.ndarray] = None
    y: Optional[np.ndarray] = None
    success: bool = False
    out_csv: Optional[str] = None
    rows: int = 0
    elapsed_s: float = 0.0
    error: Optional[str] = None


def _make_filename() -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"OES_Data_{ts}.csv"


def _resolve_worker_command() -> List[str]:
    """
    워커 실행 커맨드(리스트)를 반환한다.
    우선순위:
    1) 환경변수 OES_WORKER_EXE
    2) 고정 배치 경로: C:\\Users\\vanam\\Desktop\\OES\\oes_worker.exe
    3) 배포(onefolder) 기준 실행 폴더 내 apps/oes_service/oes_worker.exe
    4) 소스 트리 기준 <project>/apps/oes_service/oes_worker.exe
    5) 개발용 fallback: python apps/oes_service/oes_api.py
    """
    # 1) 환경변수 (최우선)
    env = os.environ.get("OES_WORKER_EXE", "").strip()
    if env:
        p = Path(env)
        if p.is_file():
            return [str(p)]

    # 2) ✅ 너가 지정한 고정 경로
    fixed = Path(r"C:\Users\vanam\Desktop\OES\oes_worker.exe")
    if fixed.is_file():
        return [str(fixed)]

    # 3) 배포(onefolder) 기준 후보
    exe_dir = Path(sys.argv[0]).resolve().parent
    cands = [
        exe_dir / "apps" / "oes_service" / "oes_worker.exe",
        exe_dir / "_internal" / "apps" / "oes_service" / "oes_worker.exe",
    ]

    # 4) 소스 트리 기준 후보 + 개발용 python fallback
    try:
        root = Path(__file__).resolve().parents[1]
        cands += [
            root / "apps" / "oes_service" / "oes_worker.exe",
            root / "_internal" / "apps" / "oes_service" / "oes_worker.exe",
        ]
        script = root / "apps" / "oes_service" / "oes_api.py"
    except Exception:
        script = None

    for p in cands:
        if p.is_file():
            return [str(p)]

    # 5) 개발용 fallback (exe 없을 때만)
    if script and script.is_file():
        return [sys.executable, str(script)]

    # 최후: PATH에서 찾도록 시도
    return ["oes_worker.exe"]


async def _drain_stream(stream: asyncio.StreamReader) -> None:
    try:
        while True:
            line = await stream.readline()
            if not line:
                return
    except Exception:
        return


class OESAsync:
    """
    메인 프로그램용 OES 클라이언트.
    """

    def __init__(
        self,
        *,
        chamber: int = 1,
        usb_index: Optional[int] = None,
        dll_path: Optional[str] = None,
        sample_interval_s: float = 1.0,
        avg_count: int = 3,
        debug_print: bool = False,
    ):
        self._ch = int(chamber)
        self._usb = int(usb_index) if usb_index is not None else (0 if self._ch == 1 else 1)
        self._dll_path = dll_path
        self._sample_interval_s = float(sample_interval_s)
        self._avg_count = int(max(1, avg_count))
        self._debug = bool(debug_print)

        self._local_dir = _LOCAL_BASE / f"CH{self._ch}"
        self._local_dir.mkdir(parents=True, exist_ok=True)

        self._nas_dir = _NAS_BASE_CH1 if self._ch == 1 else _NAS_BASE_CH2
        self._worker_cmd = _resolve_worker_command()

        self._ev_q: asyncio.Queue[OESEvent] = asyncio.Queue(maxsize=2048)

        self.is_running: bool = False
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._tail_task: Optional[asyncio.Task] = None
        self._stdout_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None

        self._out_csv_local: Optional[Path] = None
        self._x_axis: Optional[np.ndarray] = None
        self._rows_seen: int = 0

    def _emit(self, ev: OESEvent) -> None:
        try:
            self._ev_q.put_nowait(ev)
        except asyncio.QueueFull:
            pass

    async def _status(self, msg: str) -> None:
        self._emit(OESEvent(kind="status", message=msg))

    async def events(self) -> AsyncGenerator[OESEvent, None]:
        while True:
            ev = await self._ev_q.get()
            yield ev

    async def initialize_device(self) -> bool:
        cmd = [
            *self._worker_cmd,
            "--cmd", "init",
            "--ch", str(self._ch),
            "--usb", str(self._usb),
        ]
        if self._dll_path:
            cmd += ["--dll_path", str(self._dll_path)]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                creationflags=(getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0),
            )
            assert proc.stdout and proc.stderr
            stderr_task = asyncio.create_task(_drain_stream(proc.stderr))
            ok = False
            try:
                while True:
                    line = await proc.stdout.readline()
                    if not line:
                        break
                    try:
                        obj = json.loads(line.decode("utf-8", errors="ignore").strip())
                    except Exception:
                        continue
                    if obj.get("kind") == "init":
                        ok = bool(obj.get("ok", False))
                        break
                await proc.wait()
            finally:
                stderr_task.cancel()
                with contextlib.suppress(Exception):
                    await stderr_task

            await self._status(f"[OES] init {'OK' if ok else 'FAIL'} (CH{self._ch}/USB{self._usb})")
            return ok

        except Exception as e:
            await self._status(f"[OES] init 예외: {type(e).__name__}: {e}")
            return False

    async def run_measurement(self, duration_sec: float, integration_ms: int) -> None:
        if self.is_running:
            await self._status("[OES] 이미 측정 중 → 요청 무시")
            return

        out_csv = self._local_dir / _make_filename()
        self._out_csv_local = out_csv
        self._x_axis = None
        self._rows_seen = 0

        cmd = [
            *self._worker_cmd,
            "--cmd", "measure",
            "--ch", str(self._ch),
            "--usb", str(self._usb),
            "--duration", str(float(duration_sec)),
            "--integration_ms", str(int(integration_ms)),
            "--sample_interval_s", str(float(self._sample_interval_s)),
            "--avg_count", str(int(self._avg_count)),
            "--out_csv", str(out_csv),
        ]
        if self._dll_path:
            cmd += ["--dll_path", str(self._dll_path)]

        t0 = time.time()
        self.is_running = True

        try:
            self._proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                creationflags=(getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0),
            )
            assert self._proc.stdout and self._proc.stderr

            self._stderr_task = asyncio.create_task(_drain_stream(self._proc.stderr))
            self._stdout_task = asyncio.create_task(self._watch_worker_stdout(self._proc.stdout))
            self._tail_task = asyncio.create_task(self._tail_csv(out_csv))

            await self._status(f"[OES] 측정 시작: {duration_sec/60:.1f}분, out={out_csv}")

            timeout = max(10.0, float(duration_sec) + 60.0)
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                await self._status("[OES] 워커 timeout → 강제 종료")
                with contextlib.suppress(Exception):
                    self._proc.kill()
                await self._proc.wait()

            rc = int(self._proc.returncode or 0)

            if self._stdout_task:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(self._stdout_task, timeout=2.0)

            await self._stop_tail()

            elapsed = time.time() - t0
            ok = (rc == 0 and out_csv.exists() and out_csv.stat().st_size > 0)

            moved_path = None
            if ok:
                moved_path = await self._move_to_nas(out_csv)

            if ok and moved_path:
                self._emit(OESEvent(kind="finished", success=True, message="OES 측정 완료", out_csv=str(moved_path), rows=self._rows_seen, elapsed_s=elapsed))
            elif ok and not moved_path:
                self._emit(OESEvent(kind="finished", success=True, message="OES 측정 완료(로컬 저장, NAS 이동 실패)", out_csv=str(out_csv), rows=self._rows_seen, elapsed_s=elapsed))
            else:
                self._emit(OESEvent(kind="finished", success=False, message=f"OES 측정 실패(rc={rc})", out_csv=str(out_csv), rows=self._rows_seen, elapsed_s=elapsed, error=f"worker rc={rc}"))

        except Exception as e:
            elapsed = time.time() - t0
            self._emit(OESEvent(kind="finished", success=False, message="OES 예외", out_csv=str(self._out_csv_local) if self._out_csv_local else None, rows=self._rows_seen, elapsed_s=elapsed, error=f"{type(e).__name__}: {e}"))
            await self._status(f"[OES] 예외: {type(e).__name__}: {e}\n{traceback.format_exc()}")

        finally:
            await self._cleanup_proc_tasks()
            self.is_running = False

    async def stop_measurement(self) -> None:
        if self._proc and self.is_running:
            await self._status("[OES] stop_measurement → terminate")
            with contextlib.suppress(Exception):
                self._proc.terminate()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._proc.wait(), timeout=5.0)
            if self._proc.returncode is None:
                with contextlib.suppress(Exception):
                    self._proc.kill()
                    await self._proc.wait()
        await self._stop_tail()
        await self._cleanup_proc_tasks()
        self.is_running = False

    async def cleanup(self) -> None:
        await self.stop_measurement()

    async def _watch_worker_stdout(self, stream: asyncio.StreamReader) -> None:
        try:
            while True:
                line = await stream.readline()
                if not line:
                    return
                try:
                    obj = json.loads(line.decode("utf-8", errors="ignore").strip())
                except Exception:
                    continue
                k = obj.get("kind")
                if k == "started":
                    out_csv = obj.get("out_csv")
                    if out_csv:
                        await self._status(f"[OES] worker started: {out_csv}")
                elif k in ("finished", "fatal"):
                    ok = bool(obj.get("ok", False))
                    if ok:
                        await self._status("[OES] worker finished OK")
                    else:
                        err = obj.get("error", "")
                        await self._status(f"[OES] worker finished FAIL: {err}")
        except Exception:
            return

    async def _tail_csv(self, path: Path) -> None:
        for _ in range(300):  # 최대 30초
            if path.exists() and path.stat().st_size > 0:
                break
            await asyncio.sleep(0.1)

        if not path.exists():
            return

        try:
            with open(path, "r", encoding="utf-8", newline="") as f:
                while True:
                    line = f.readline()
                    if not line:
                        await asyncio.sleep(0.1)
                        continue
                    header = [c.strip() for c in line.strip().split(",")]
                    break

                if not header or len(header) < 2:
                    return

                xs = []
                for c in header[1:]:
                    try:
                        xs.append(float(c))
                    except Exception:
                        pass
                self._x_axis = np.asarray(xs, dtype=float) if xs else np.arange(max(0, len(header) - 1), dtype=float)

                while True:
                    line = f.readline()
                    if not line:
                        await asyncio.sleep(0.1)
                        continue
                    parts = [c.strip() for c in line.strip().split(",")]
                    if len(parts) < 2:
                        continue

                    ys = []
                    ok = True
                    for c in parts[1:]:
                        try:
                            ys.append(float(c))
                        except Exception:
                            ok = False
                            break
                    if not ok:
                        continue

                    if self._x_axis is None:
                        self._x_axis = np.arange(len(ys), dtype=float)

                    n = min(len(self._x_axis), len(ys))
                    x = self._x_axis[:n]
                    y = np.asarray(ys[:n], dtype=float)

                    self._rows_seen += 1
                    self._emit(OESEvent(kind="data", x=x, y=y))

        except asyncio.CancelledError:
            return
        except Exception:
            return

    async def _stop_tail(self) -> None:
        if self._tail_task:
            self._tail_task.cancel()
            with contextlib.suppress(Exception):
                await self._tail_task
            self._tail_task = None

    async def _cleanup_proc_tasks(self) -> None:
        for name in ("_stdout_task", "_stderr_task"):
            t = getattr(self, name)
            if t:
                t.cancel()
                with contextlib.suppress(Exception):
                    await t
                setattr(self, name, None)

        if self._proc and self._proc.returncode is None:
            with contextlib.suppress(Exception):
                self._proc.kill()
                await self._proc.wait()
        self._proc = None

    async def _move_to_nas(self, local_csv: Path, timeout_s: float = 30.0) -> Optional[Path]:
        nas_dir = self._nas_dir
        dest = nas_dir / local_csv.name

        try:
            nas_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            await self._status("[OES] NAS 폴더 생성 실패 → 로컬 유지")
            return None

        if os.name == "nt":
            cmd = [
                "robocopy",
                str(local_csv.parent),
                str(nas_dir),
                local_csv.name,
                "/R:1", "/W:1",
                "/NFL", "/NDL", "/NJH", "/NJS", "/NP",
            ]
            try:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                try:
                    await asyncio.wait_for(proc.wait(), timeout=timeout_s)
                except asyncio.TimeoutError:
                    with contextlib.suppress(Exception):
                        proc.kill()
                    await self._status("[OES] NAS 복사 timeout → 로컬 유지")
                    return None

                rc = int(proc.returncode or 0)
                if rc >= 8:
                    await self._status(f"[OES] NAS 복사 실패(rc={rc}) → 로컬 유지")
                    return None
            except Exception as e:
                await self._status(f"[OES] NAS 복사 예외({type(e).__name__}) → 로컬 유지")
                return None
        else:
            try:
                import shutil
                shutil.copy2(local_csv, dest)
            except Exception:
                return None

        try:
            if not dest.exists():
                return None
            if dest.stat().st_size != local_csv.stat().st_size:
                return None
        except Exception:
            return None

        try:
            local_csv.unlink()
        except Exception:
            pass

        await self._status(f"[OES] NAS 이동 완료: {dest}")
        return dest

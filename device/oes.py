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
import csv
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
_LOCAL_BASE = Path(os.environ.get("OES_LOCAL_BASE", r"C:\Users\vanam\Desktop\OES"))
# ====================================

EventKind = Literal["status", "data", "finished"]


def _env_flag(name: str, default: str = "0") -> bool:
    v = os.environ.get(name, default).strip().lower()
    return v in {"1", "true", "yes", "y", "on"}

def _worker_creationflags() -> int:
    # ✅ 기본: 콘솔 창 보이기
    # 숨기고 싶으면 OES_WORKER_HIDE_CONSOLE=1
    if os.name == "nt" and _env_flag("OES_WORKER_HIDE_CONSOLE", "0"):
        return int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
    return 0


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


def _main_exe_dir() -> Path:
    """메인 공정 프로그램.exe가 있는 폴더"""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(sys.argv[0]).resolve().parent


def _resolve_worker_command() -> List[str]:
    """
    메인 공정 프로그램.exe 폴더 기준(하드코딩 디렉토리 유지)
      <main_exe_dir>\apps\oes_service\{oes_worker.exe 또는 oes_api.exe}
    """
    base = _main_exe_dir()
    svc_dir = base / "apps" / "oes_service"

    # ✅ 1순위: oes_worker.exe
    exe1 = svc_dir / "oes_worker.exe"
    if exe1.exists():
        return [str(exe1)]

    # ✅ 2순위: oes_api.exe (빌드/이름 불일치 대비)
    exe2 = svc_dir / "oes_api.exe"
    if exe2.exists():
        return [str(exe2)]

    # 없으면 기존 기대값(로그에서 expected로 보여주기 위함)
    return [str(exe1)]


async def _drain_stream(
    stream: asyncio.StreamReader,
    sink: Optional[List[str]] = None,
    *,
    max_lines: int = 80,
) -> None:
    try:
        while True:
            line = await stream.readline()
            if not line:
                return
            if sink is not None:
                s = line.decode("utf-8", errors="ignore").rstrip()
                if s:
                    sink.append(s)
                    # ✅ 마지막 max_lines만 유지
                    if len(sink) > max_lines:
                        del sink[: len(sink) - max_lines]
    except asyncio.CancelledError:
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

        # stderr 마지막 N줄(라인 단위)만 보관
        self._stderr_tail: List[str] = []
        self._stderr_tail_max_lines: int = 50  # 원하는 값

        # ✅ 워커가 stdout으로 준 최종 결과(JSON)를 저장
        self._worker_finished: Optional[dict] = None

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

    async def drain_events(self, max_items: int = 10000) -> int:
        """이전 run의 이벤트 찌꺼기를 비워 다음 run 오동작을 방지."""
        n = 0
        while n < max_items:
            try:
                self._ev_q.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                n += 1
        return n

    async def initialize_device(self) -> bool:
        # ✅ 워커 경로는 매번 재계산(배치 바뀌는 경우 디버깅에 유리)
        self._worker_cmd = _resolve_worker_command()

        cmd = [
            *self._worker_cmd,
            "--cmd", "init",
            "--ch", str(self._ch),
            "--usb", str(self._usb),
            # ✅ 중요: 워커 init도 out_dir를 받게 해서 "vanam 고정경로" 문제 제거
            "--out_dir", str(self._local_dir),
        ]
        if self._dll_path:
            cmd += ["--dll_path", str(self._dll_path)]

        worker_exe = Path(self._worker_cmd[0])
        await self._status(
            "[OES] init spawn\n"
            f"  worker={worker_exe}\n"
            f"  exists={worker_exe.exists()}\n"
            f"  cmd={cmd}\n"
            f"  base_dir={_main_exe_dir()}\n"
            f"  cwd={Path.cwd()}\n"
            f"  argv0={sys.argv[0]}\n"
            f"  sys.executable={sys.executable}"
        )
        if not worker_exe.exists():
            await self._status("[OES] 워커 exe 없음 → init 실패")
            return False

        # ✅ init 무한대기 방지
        init_timeout_s = float(os.environ.get("OES_INIT_TIMEOUT_S", "20"))

        creationflags = _worker_creationflags()

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                creationflags=creationflags,
            )
            assert proc.stdout and proc.stderr

            # ✅ init에서도 stderr tail 저장
            self._stderr_tail.clear()
            stderr_task = asyncio.create_task(
                _drain_stream(proc.stderr, self._stderr_tail, max_lines=self._stderr_tail_max_lines)
            )

            async def _wait_init() -> bool:
                while True:
                    line = await proc.stdout.readline()
                    if not line:
                        return False
                    try:
                        obj = json.loads(line.decode("utf-8", errors="ignore").strip())
                    except Exception:
                        continue
                    if obj.get("kind") == "init":
                        return bool(obj.get("ok", False))
                    # 워커가 status를 주면 그대로 기록
                    if obj.get("kind") == "status":
                        msg = obj.get("message", "")
                        if msg:
                            await self._status(f"[OES] worker: {msg}")

            try:
                ok = await asyncio.wait_for(_wait_init(), timeout=init_timeout_s)
            except asyncio.TimeoutError:
                ok = False
                await self._status(f"[OES] init TIMEOUT({init_timeout_s}s) → kill")
                with contextlib.suppress(Exception):
                    proc.kill()
            finally:
                with contextlib.suppress(Exception):
                    await proc.wait()
                stderr_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await stderr_task

            rc = int(proc.returncode or 0)
            if (not ok) or rc != 0:
                if self._stderr_tail:
                    await self._status("[OES] init stderr_tail:\n" + "\n".join(self._stderr_tail[-50:]))
                await self._status(f"[OES] init FAIL rc={rc}")
            else:
                await self._status("[OES] init OK")

            return bool(ok)

        except Exception as e:
            await self._status(f"[OES] init 예외: {type(e).__name__}: {e}")
            if self._stderr_tail:
                await self._status("[OES] init stderr_tail:\n" + "\n".join(self._stderr_tail[-50:]))
            return False

    async def run_measurement(self, duration_sec: float, integration_ms: int) -> None:
        if self.is_running:
            await self._status("[OES] 이미 측정 중 → 요청 무시")
            return
        
        # ✅ measure 직전에도 워커 경로 재계산
        self._worker_cmd = _resolve_worker_command()

        out_csv = self._local_dir / _make_filename()
        self._out_csv_local = out_csv
        self._x_axis = None
        self._rows_seen = 0
        
        # ✅ 이번 측정의 워커 결과 초기화
        self._worker_finished = None

        worker_exe = Path(self._worker_cmd[0])
        if not worker_exe.exists():
            await self._status(
                "[OES] 워커 exe를 찾지 못함 → 측정 스킵\n"
                f" expected={worker_exe}\n"
                f" base_dir={_main_exe_dir()}\n"
                f" cwd={Path.cwd()}"
            )
            # ✅ finished 이벤트로 실패만 알리고 끝(공정은 다음 단계로 계속)
            self._emit(OESEvent(
                kind="finished",
                success=False,
                message="OES 측정 실패(워커 exe 없음)",
                out_csv=str(self._out_csv_local) if self._out_csv_local else None,
                rows=0,
                elapsed_s=0.0,
                error=f"worker exe not found: {worker_exe}",
            ))
            self.is_running = False
            return

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
            creationflags = _worker_creationflags()
            await self._status(
                "[OES] measure spawn\n"
                f"  cmd={cmd}\n"
                f"  creationflags={creationflags}\n"
                f"  out_csv={out_csv}\n"
                f"  cwd={Path.cwd()}"
            )

            self._proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                creationflags=creationflags,
            )

            assert self._proc.stdout and self._proc.stderr

            self._stderr_tail.clear()
            self._stderr_task = asyncio.create_task(
                _drain_stream(self._proc.stderr, self._stderr_tail, max_lines=self._stderr_tail_max_lines)
            )
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

            # ✅ 1) 워커 finished JSON이 있으면 그 결과를 최우선 사용
            worker_ok = None
            worker_rows = None
            worker_elapsed = None
            worker_err = None

            if isinstance(self._worker_finished, dict):
                if self._worker_finished.get("kind") in ("finished", "fatal"):
                    worker_ok = bool(self._worker_finished.get("ok", False))
                    worker_rows = self._worker_finished.get("rows", None)
                    worker_elapsed = self._worker_finished.get("elapsed_s", None)
                    worker_err = self._worker_finished.get("error", None)

            # ✅ 2) fallback: 워커 결과를 못 받았을 때만 기존 판정 사용
            if worker_ok is None:
                ok = (rc == 0 and out_csv.exists() and out_csv.stat().st_size > 0)
            else:
                ok = worker_ok

            final_rows = int(worker_rows) if isinstance(worker_rows, (int, float)) else int(self._rows_seen)
            final_elapsed = float(worker_elapsed) if isinstance(worker_elapsed, (int, float)) else float(elapsed)

            nas_ok = False
            nas_csv = None
            nas_error = None
            local_deleted = False

            if isinstance(self._worker_finished, dict):
                nas_ok = bool(self._worker_finished.get("nas_ok", False))
                nas_csv = self._worker_finished.get("nas_csv")
                nas_error = self._worker_finished.get("nas_error")
                local_deleted = bool(self._worker_finished.get("local_deleted", False))

            final_path = out_csv
            msg = "OES 측정 완료"

            if ok and nas_ok and nas_csv:
                final_path = Path(nas_csv)
                msg = "OES 측정 완료(NAS 저장)"
            elif ok and (not nas_ok):
                # 워커가 NAS 복사를 시도했는데 실패한 경우(또는 워커가 구버전이라 필드 없음)
                if nas_error:
                    msg = f"OES 측정 완료(로컬 저장, NAS 실패: {nas_error})"
                else:
                    msg = "OES 측정 완료(로컬 저장, NAS 미확인)"

            # 워커가 로컬 삭제를 못했으면(메인이 열고 있었을 가능성) tail 종료 후 여기서 삭제 시도
            if ok and nas_ok and out_csv.exists() and (not local_deleted):
                with contextlib.suppress(Exception):
                    out_csv.unlink()
                    local_deleted = True

            # 실패 시 에러 메시지 구성(워커 error + stderr tail)
            err_msg = None
            if not ok:
                parts = []
                if worker_err:
                    parts.append(str(worker_err))
                if self._stderr_tail:
                    parts.append("stderr_tail:\n" + "\n".join(self._stderr_tail[-50:]))
                if not parts:
                    parts.append(f"worker rc={rc}")
                err_msg = "\n".join(parts)

            self._emit(OESEvent(
                kind="finished",
                success=bool(ok),
                message=msg if ok else f"OES 측정 실패(rc={rc})",
                out_csv=str(final_path),
                rows=final_rows,
                elapsed_s=final_elapsed,
                error=err_msg,
            ))

        except Exception as e:
            elapsed = time.time() - t0
            self._emit(OESEvent(kind="finished", success=False, message="OES 예외", out_csv=str(self._out_csv_local) if self._out_csv_local else None, rows=self._rows_seen, elapsed_s=elapsed, error=f"{type(e).__name__}: {e}"))
            await self._status(f"[OES] 예외: {type(e).__name__}: {e}\n{traceback.format_exc()}")

        finally:
            # ✅ 예외/타임아웃/강제종료 어떤 경우에도 tail은 반드시 정리
            await self._stop_tail()
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
                if k == "status":
                    msg = obj.get("message", "")
                    if msg:
                        await self._status(f"[OES] {msg}")

                elif k == "started":
                    # ✅ 워커가 첫 프레임 확보 + CSV 헤더 작성 완료했다는 신호
                    out_csv = obj.get("out_csv")
                    cols = obj.get("cols")
                    resolved_usb = obj.get("resolved_usb")
                    await self._status(f"[OES] worker started out_csv={out_csv} cols={cols} usb={resolved_usb}")

                elif k in ("finished", "fatal"):
                    self._worker_finished = obj
                    ok = bool(obj.get("ok", False))
                    if ok:
                        await self._status("[OES] worker finished OK")
                    else:
                        err = obj.get("error", "")
                        await self._status(f"[OES] worker finished FAIL: {err}")

        except Exception:
            return

    async def _tail_csv(self, path: Path) -> None:
        t0 = time.time()
        last_log = 0.0
        while True:
            if path.exists() and path.stat().st_size > 0:
                break

            # 워커가 먼저 죽었는데 CSV가 없으면 원인 로그
            if self._proc and (self._proc.returncode is not None):
                await self._status(f"[OES] CSV 미생성 상태로 워커 종료됨 rc={self._proc.returncode} path={path}")
                return

            # 5초마다 진행상황 로그
            if (time.time() - last_log) > 5.0:
                last_log = time.time()
                await self._status(f"[OES] CSV 생성 대기중... {(last_log - t0):.1f}s path={path}")

            # 60초 넘어가면 일단 종료(원인 파악용)
            if (time.time() - t0) > 60.0:
                await self._status(f"[OES] CSV 생성 대기 TIMEOUT(60s) path={path}")
                return

            await asyncio.sleep(0.1)

        try:
            with open(path, "r", encoding="utf-8", newline="") as f:
                while True:
                    line = f.readline()
                    if not line:
                        await asyncio.sleep(0.1)
                        continue
                    try:
                        row = next(csv.reader([line]))
                    except Exception:
                        continue  # ✅ 헤더 줄이 깨졌으면 다음 줄 시도
                    header = [c.strip() for c in row]
                    break

                if not header or len(header) < 2:
                    return

                xs = []
                parse_err = 0
                for c in header[1:]:
                    try:
                        xs.append(float(c))
                    except Exception:
                        parse_err += 1

                # ✅ 하나라도 실패하면 "부분 x축"을 쓰지 말고 안전하게 인덱스로 통일(길이 보장)
                if parse_err > 0:
                    self._x_axis = np.arange(max(0, len(header) - 1), dtype=float)
                else:
                    self._x_axis = np.asarray(xs, dtype=float) if xs else np.arange(max(0, len(header) - 1), dtype=float)

                last_data_t = time.time()

                while True:
                    line = f.readline()
                    if not line:
                        # ✅ 워커가 끝났고(프로세스 종료), 3초 동안 새 데이터가 없으면 tail 종료
                        if self._proc and (self._proc.returncode is not None):
                            if (time.time() - last_data_t) > 3.0:
                                return
                        await asyncio.sleep(0.1)
                        continue

                    last_data_t = time.time()
                    try:
                        row = next(csv.reader([line]))
                    except Exception:
                        continue  # ✅ 해당 줄만 스킵하고 계속 tail
                    parts = [c.strip() for c in row]

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
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._tail_task
            self._tail_task = None

    async def _cleanup_proc_tasks(self) -> None:
        for name in ("_stdout_task", "_stderr_task"):
            t = getattr(self, name)
            if t:
                t.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await t
                setattr(self, name, None)

        if self._proc and self._proc.returncode is None:
            with contextlib.suppress(Exception):
                self._proc.kill()
                await self._proc.wait()
        self._proc = None

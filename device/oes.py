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

# ====== OES 로컬 저장 경로 ======
# 1) 환경변수 OES_LOCAL_BASE가 있으면 그 값을 사용
# 2) 없으면 "현재 로그인 사용자" Desktop\OES 를 기본값으로 사용
_LOCAL_BASE = Path(os.environ.get("OES_LOCAL_BASE", str(Path.home() / "Desktop" / "OES")))
# ====================================

EventKind = Literal["status", "data", "finished"]


def _env_flag(name: str, default: str = "0") -> bool:
    v = os.environ.get(name, default).strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def _worker_creationflags() -> int:
    """
    - 기본: 부모 프로세스 콘솔 상속(creationflags=0)
    - OES_WORKER_SHOW_CONSOLE=1 : 워커를 새 콘솔창으로 실행(디버깅용)
    - OES_WORKER_HIDE_CONSOLE=1 : 콘솔 숨김(배포용)
    """
    if os.name != "nt":
        return 0

    if _env_flag("OES_WORKER_HIDE_CONSOLE", "0"):
        return int(getattr(subprocess, "CREATE_NO_WINDOW", 0))

    if _env_flag("OES_WORKER_SHOW_CONSOLE", "0"):
        return int(getattr(subprocess, "CREATE_NEW_CONSOLE", 0))

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
        try:
            self._local_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            # Desktop 접근/권한 문제 시 LocalAppData로 폴백 (프로그램이 죽지 않게)
            base = Path(os.environ.get("LOCALAPPDATA", str(Path.home()))) / "VanaM" / "OES"
            self._local_dir = base / f"CH{self._ch}"
            self._local_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            # 최후 폴백: 실행 폴더 아래 _OES
            base = _main_exe_dir() / "_OES"
            self._local_dir = base / f"CH{self._ch}"
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
        self._stop_requested: bool = False   # ✅ 항상 존재하도록 초기화

        # stderr 마지막 N줄(라인 단위)만 보관
        self._stderr_tail: List[str] = []
        self._stderr_tail_max_lines: int = 50  # 원하는 값

        # ✅ 워커가 stdout으로 준 최종 결과(JSON)를 저장
        self._worker_finished: Optional[dict] = None

        # ✅ init(사전점검) 캐시/동기화 상태 (initialize_device에서 사용)
        self._init_lock = asyncio.Lock()
        self._init_task: Optional[asyncio.Task] = None
        self._init_done: bool = False
        self._init_ok: bool = False
        self._init_result: Optional[dict] = None
        self._init_error: Optional[str] = None

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

    async def initialize_device(self, *, timeout_s: float = 20.0, force: bool = False) -> bool:
        """
        워커(cmd=init)를 한 번 실행해서 장치/드라이버/DLL 상태를 사전 점검한다.
        - 성공 시: self._init_ok=True 로 캐시
        - 실패 시: self._init_error / scan 정보 로그로 남김
        """
        # ✅ init 직전에도 워커 경로 재계산(측정과 동일)
        self._worker_cmd = _resolve_worker_command()

        if self._init_done and self._init_ok and not force:
            return True

        async with self._init_lock:
            if self._init_task and not self._init_task.done():
                return await self._init_task

            self._init_task = asyncio.create_task(self._initialize_device_impl(timeout_s=timeout_s, force=force))
            return await self._init_task


    async def _initialize_device_impl(self, *, timeout_s: float, force: bool) -> bool:
        cmd = [*self._worker_cmd, "--cmd", "init", "--ch", str(self._ch), "--usb", str(self._usb)]
        if self._dll_path:
            cmd += ["--dll_path", str(self._dll_path)]

        self._init_done = False
        self._init_ok = False
        self._init_result = None
        self._init_error = None

        stderr_tail: List[str] = []
        init_obj: Optional[dict] = None
        proc: Optional[asyncio.subprocess.Process] = None

        creationflags = _worker_creationflags()

        try:
            await self._status(f"[OES] init spawn ch={self._ch} usb={self._usb} cmd={cmd}")

            worker_exe = Path(self._worker_cmd[0])
            worker_dir = worker_exe.resolve().parent

            env = os.environ.copy()
            env.setdefault("PYTHONUNBUFFERED", "1")

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(worker_dir),
                env=env,
                creationflags=creationflags,
            )

            stderr_task = asyncio.create_task(_drain_stream(proc.stderr, stderr_tail, max_lines=80))

            deadline = asyncio.get_event_loop().time() + float(timeout_s)
            while True:
                remain = deadline - asyncio.get_event_loop().time()
                if remain <= 0:
                    raise asyncio.TimeoutError()

                line = await asyncio.wait_for(proc.stdout.readline(), timeout=remain)
                if not line:
                    break

                s = line.decode("utf-8", errors="replace").strip()
                if not s:
                    continue

                # 워커의 status도 그대로 로그에 남김
                obj = None
                try:
                    obj = json.loads(s)
                except Exception:
                    await self._status(f"[OES][worker] {s}")
                    continue

                kind = obj.get("kind")
                if kind == "status":
                    await self._status(str(obj.get("message", "")))
                elif kind == "init":
                    init_obj = obj
                    break
                else:
                    await self._status(f"[OES][worker] {obj}")

            try:
                await asyncio.wait_for(proc.wait(), timeout=3.0)
            except Exception:
                with contextlib.suppress(Exception):
                    proc.kill()
                with contextlib.suppress(Exception):
                    await proc.wait()

            with contextlib.suppress(Exception):
                await stderr_task

            rc = proc.returncode
            if not init_obj:
                self._init_error = f"init JSON not received (rc={rc})"
                if stderr_tail:
                    self._init_error += " | stderr_tail=" + " / ".join(stderr_tail[-5:])
                await self._status(f"[OES] init FAIL: {self._init_error}")
                self._init_done = True
                return False

            ok = bool(init_obj.get("ok", False))
            if ok:
                self._init_ok = True
                self._init_result = init_obj
                await self._status(f"[OES] init OK: {init_obj}")
                self._init_done = True
                return True

            # ok=False → error/scan을 최대한 보여준다
            err = init_obj.get("error") or "initialize_device failed"
            scan = init_obj.get("scan") or {}
            self._init_error = f"{err} | scan={scan}"
            if stderr_tail:
                self._init_error += " | stderr_tail=" + " / ".join(stderr_tail[-5:])

            await self._status(f"[OES] init FAIL: {self._init_error}")
            self._init_result = init_obj
            self._init_done = True
            return False

        except asyncio.TimeoutError:
            if proc is not None and proc.returncode is None:
                with contextlib.suppress(Exception):
                    proc.kill()
                with contextlib.suppress(Exception):
                    await proc.wait()

            self._init_error = f"init timeout after {timeout_s}s"
            if stderr_tail:
                self._init_error += " | stderr_tail=" + " / ".join(stderr_tail[-5:])
            await self._status(f"[OES] init TIMEOUT: {self._init_error}")
            self._init_done = True
            return False

        except Exception as e:
            if proc is not None and proc.returncode is None:
                with contextlib.suppress(Exception):
                    proc.kill()
                with contextlib.suppress(Exception):
                    await proc.wait()

            self._init_error = f"{type(e).__name__}: {e}"
            await self._status(f"[OES] init EXC: {self._init_error}")
            self._init_done = True
            return False

    async def run_measurement(self, duration_sec: float, integration_ms: int) -> None:
        if self.is_running:
            await self._status("[OES] 이미 측정 중 → 요청 무시")
            return

        # ✅ measure 직전에도 워커 경로 재계산
        self._worker_cmd = _resolve_worker_command()

        # ✅ 이전 run 찌꺼기(이벤트/stop flag/유령 워커) 정리
        with contextlib.suppress(Exception):
            await self._stop_tail()
        with contextlib.suppress(Exception):
            await self.drain_events()

        # (1) 이전 STOP flag가 남아있으면 워커가 시작 직후 바로 종료될 수 있음 → 제거
        stop_usb_flag = self._local_dir / f".stop_usb{int(self._usb)}.flag"
        with contextlib.suppress(Exception):
            stop_usb_flag.unlink()
        with contextlib.suppress(Exception):
            for p in self._local_dir.glob("*.stop"):
                with contextlib.suppress(Exception):
                    p.unlink()

        # (2) run_measurement가 아닌데 워커 프로세스가 남아있으면(유령) → 종료 후 재시작
        if self._proc:
            if self._proc.returncode is None:
                await self._status("[OES] 이전 워커 프로세스가 남아있음 → 정리 후 재시작")
                with contextlib.suppress(Exception):
                    await self.stop_measurement()
            with contextlib.suppress(Exception):
                await self._cleanup_proc_tasks()

        out_csv = self._local_dir / _make_filename()
        self._out_csv_local = out_csv
        self._x_axis = None
        self._rows_seen = 0

        # ✅ STOP 상태 초기화
        self._stop_requested = False

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

            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            worker_dir = str(Path(self._worker_cmd[0]).resolve().parent)

            self._proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                creationflags=creationflags,
                cwd=worker_dir,
                env=env,
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
        proc = self._proc
        if not proc:
            await self._stop_tail()
            return

        # ✅ 프로세스가 이미 끝났으면 tail/태스크만 정리하고 종료
        if proc.returncode is not None:
            await self._stop_tail()
            # run_measurement가 돌고 있지 않은 '유령' 상태면 남은 태스크/프로세스 핸들까지 정리
            if not self.is_running:
                with contextlib.suppress(Exception):
                    await self._cleanup_proc_tasks()
            return

        self._stop_requested = True
        await self._status(f"[OES] stop_measurement → request stop flag (pid={getattr(proc, 'pid', None)})")

        # ✅ 1) tail을 먼저 멈춰서 파일 핸들을 최대한 빨리 풀어준다(워커 로컬삭제 가능하게)
        await self._stop_tail()

        # ✅ 2) stop flag 생성 + (중요) 나중에 지울 수 있게 경로를 잡아둠
        stop_usb = self._local_dir / f".stop_usb{int(self._usb)}.flag"
        stop_csv: Optional[Path] = None
        if self._out_csv_local:
            stop_usb = self._out_csv_local.parent / f".stop_usb{int(self._usb)}.flag"
            stop_csv = self._out_csv_local.with_suffix(self._out_csv_local.suffix + ".stop")

        with contextlib.suppress(Exception):
            stop_usb.write_text("stop\\n", encoding="utf-8")
        if stop_csv:
            with contextlib.suppress(Exception):
                stop_csv.write_text("stop\\n", encoding="utf-8")

        # ✅ 3) 워커가 NAS 업로드/정리 후 종료할 시간을 준다
        try:
            await asyncio.wait_for(proc.wait(), timeout=60.0)
        except asyncio.TimeoutError:
            await self._status("[OES] stop: graceful timeout → terminate/kill fallback")
            with contextlib.suppress(Exception):
                proc.terminate()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            if proc.returncode is None:
                with contextlib.suppress(Exception):
                    proc.kill()
                with contextlib.suppress(Exception):
                    await proc.wait()

        # ✅ stdout watcher가 finished JSON을 다 읽도록 잠깐 대기(선택)
        if self._stdout_task:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._stdout_task, timeout=2.0)

        # ✅ 4) stop flag 정리(다음 측정이 막히지 않도록)
        with contextlib.suppress(Exception):
            stop_usb.unlink()
        if stop_csv:
            with contextlib.suppress(Exception):
                stop_csv.unlink()
        with contextlib.suppress(Exception):
            for p in self._local_dir.glob("*.stop"):
                with contextlib.suppress(Exception):
                    p.unlink()

        # ✅ run_measurement가 돌고 있지 않은 '유령' 상태면 여기서 프로세스/태스크까지 확실히 정리
        if not self.is_running:
            with contextlib.suppress(Exception):
                await self._cleanup_proc_tasks()

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
        # stdout/stderr task 정리
        for name in ("_stdout_task", "_stderr_task"):
            t = getattr(self, name)
            if t:
                t.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await t
                setattr(self, name, None)

        proc = self._proc
        self._proc = None

        if not proc:
            return

        # ✅ STOP 요청이 있었으면 먼저 정상 종료를 기다린다
        if proc.returncode is None and self._stop_requested:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(proc.wait(), timeout=20.0)

        # 그래도 안 죽으면 terminate→kill 순서로만 “최후 수단”
        if proc.returncode is None:
            with contextlib.suppress(Exception):
                proc.terminate()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(proc.wait(), timeout=5.0)

        if proc.returncode is None:
            with contextlib.suppress(Exception):
                proc.kill()
                await proc.wait()

# device/oes.py
# -*- coding: utf-8 -*-
"""
OES.py (Worker Client Only)

✅ 이 파일은 "메인 프로그램"에서만 사용합니다.
- OES DLL/드라이버(네이티브) 호출을 절대 하지 않습니다.
- 외부 워커( apps/oes_service/oes_api.exe )를 실행해서 측정을 수행합니다.
- 측정 중에는 로컬 CSV( C:\\Users\\vanam\\Desktop\\oes\\CH{n} )에 append로 저장되고,
  메인 프로그램은 해당 CSV를 tail 하여 실시간 그래프를 갱신합니다.
- 측정 종료 후 NAS( \\\\VanaM_NAS\\VanaM_Sputter\\OES\\CH{n} )로 "이동"(copy+검증 후 로컬 삭제)합니다.
  NAS 이동 실패 시 로컬 파일은 유지합니다.

⚠️ 중요:
- 워커(oes_api.py)는 기존 DLL 측정 코드가 필요합니다.
  따라서 기존 DLL 측정 구현은 device/oes_core.py 로 옮기고,
  apps/oes_service/oes_api.py 의 import를 `from device.oes_core import OESAsync`로 바꿔야 합니다.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator, Literal, Optional, List, Union

import numpy as np

from util.app_logging import get_app_logger
from lib.config_ch2 import DEBUG_PRINT  # 기존 프로젝트 값 유지(로그 디버그)


EventKind = Literal["status", "data", "finished"]

# ✅ 사용자 요청: 3개는 고정값
_FIXED_SAMPLE_INTERVAL_S = 1.0
_FIXED_AVG_COUNT = 3


@dataclass
class OESEvent:
    kind: EventKind
    message: Optional[str] = None
    x: Optional[List[float]] = None
    y: Optional[List[float]] = None
    success: Optional[bool] = None


def _resolve_oes_dll_path(dll_path: Optional[str]) -> Optional[str]:
    """dll_path가 None이면 None을 반환(워커가 자체 탐색). 주어졌다면 그대로 전달."""
    if not dll_path:
        env = os.getenv("OES_DLL_PATH")
        if env and Path(env).is_file():
            return str(Path(env))
        return None
    return str(dll_path)


def _nas_dir_for_ch(ch: int, save_directory: str) -> Path:
    """
    기존 시그니처(save_directory)를 최대한 유지하면서,
    - save_directory가 ...\\CH1 또는 ...\\CH2로 끝나면 부모/CH{ch}로 정규화
    - 아니면 ...\\CH{ch}를 붙여 사용
    """
    p = Path(save_directory)
    if p.name.upper() in {"CH1", "CH2"}:
        return p.parent / f"CH{int(ch)}"
    return p / f"CH{int(ch)}"


def _local_dir_for_ch(ch: int) -> Path:
    # 사용자 고정 경로
    base = Path(r"C:\Users\vanam\Desktop\oes")
    return base / f"CH{int(ch)}"


class OESAsync:
    """
    메인 프로그램용 OES 워커 클라이언트.

    ✅ 기존 메인 코드 호환 API
    - await initialize_device()
    - await run_measurement(duration_sec, integration_ms)
    - async for ev in events(): ...
    - await stop_measurement(), await cleanup()
    - (optional) drain_events()
    """

    def __init__(
        self,
        *,
        dll_path: Optional[str] = None,
        save_directory: str = r"\\VanaM_NAS\VanaM_Sputter\OES\CH2",
        sample_interval_s: float = 1.0,  # (요청) 고정값으로 덮어씀
        avg_count: int = 3,              # (요청) 고정값으로 덮어씀
        debug_print: bool = DEBUG_PRINT,
        chamber: int = 2,                 # 1→USB0, 2→USB1
        usb_index: Optional[int] = None,  # 지정 시 우선
        # (추가) 워커 exe 위치를 직접 주고 싶을 때만 사용(없으면 자동 탐색)
        worker_path: Optional[str] = None,
        # (추가) 워커 종료 여유시간(측정 duration 이후 추가 대기)
        grace_s: float = 60.0,
        tail_poll_s: float = 0.25,
    ):
        self._debug = bool(debug_print)

        self._chamber = int(chamber)
        self._usb_index = int(usb_index) if usb_index is not None else (0 if self._chamber == 1 else 1)

        # ✅ DLL 경로는 워커에 전달용(없으면 워커가 자체 탐색)
        self._dll_path = _resolve_oes_dll_path(dll_path)

        # ✅ 사용자 요구 경로
        self._local_dir = _local_dir_for_ch(self._chamber)
        self._nas_dir = _nas_dir_for_ch(self._chamber, save_directory)

        with contextlib.suppress(Exception):
            self._local_dir.mkdir(parents=True, exist_ok=True)

        # ✅ 고정값(요청 반영)
        self._sample_interval_s = _FIXED_SAMPLE_INTERVAL_S
        self._avg_count = _FIXED_AVG_COUNT

        self._worker_path = worker_path
        self._grace_s = float(grace_s)
        self._tail_poll_s = float(tail_poll_s)

        # 상태/메타
        self.sChannel: int = -1
        self._npix: int = 0
        self._model_name: str = "UNKNOWN"

        self.is_running: bool = False
        self._stop_requested: bool = False
        self._stop_reason: str = ""
        self._start_time_str: str = ""

        # 기존 코드 호환용 필드(실제 사용 X)
        self.measured_rows: list[list[Union[str, float]]] = []

        # 결과 저장 상태(워커가 만든 CSV 경로를 추적)
        self._saved_once: bool = False
        self._last_save_path: Optional[Path] = None

        # 이벤트 큐
        self._ev_q: asyncio.Queue[OESEvent] = asyncio.Queue(maxsize=256)

        # 런타임 핸들
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._tail_task: Optional[asyncio.Task] = None
        self._stdout_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._out_csv: Optional[Path] = None
        self._tail_x: Optional[List[float]] = None

    # ─────────────────────────────
    # Public API
    # ─────────────────────────────

    async def initialize_device(self) -> bool:
        """워커(--cmd init)로 장치 체크만 수행."""
        try:
            entry = self._resolve_worker_entry()

            argv = entry + [
                "--cmd", "init",
                "--ch", str(self._chamber),
                "--usb", str(self._usb_index),
            ]
            if self._dll_path:
                argv += ["--dll_path", str(self._dll_path)]

            rc, out, err = await self._run_capture(argv, timeout_s=40.0)

            payload = self._parse_last_json(out)
            if not payload or payload.get("kind") != "init":
                await self._status(f"[OES][INIT] 응답 파싱 실패 rc={rc} err={err[-200:]}")
                return False

            ok = bool(payload.get("ok", False))
            if ok:
                self.sChannel = int(payload.get("resolved_usb", self._usb_index))
                self._npix = int(payload.get("pixels", 0))
                self._model_name = str(payload.get("model", "UNKNOWN"))
                await self._status(f"[OES][INIT] OK ch={self._chamber} usb={self.sChannel} pixels={self._npix} model={self._model_name}")
                return True

            await self._status(f"[OES][INIT] FAIL: {payload}")
            return False

        except Exception as e:
            await self._status(f"[OES][INIT] 예외: {type(e).__name__}: {e}")
            if self._debug:
                traceback.print_exc()
            return False

    async def run_measurement(self, duration_sec: float, integration_time_ms: int):
        """
        워커(--cmd measure) 실행.

        ✅ chamber_runtime.py가 `await self.oes.run_measurement(...)`로 호출하므로,
           여기서는 '측정 종료까지' await 로 대기합니다.
           (측정 중 데이터는 events()로 계속 올라갑니다)
        """
        if self.is_running:
            await self._status("[OES] 측정 시작 요청 무시: 이미 실행 중")
            return

        self._reset_for_new_run()
        self.is_running = True
        self._start_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")

        started_evt = asyncio.Event()
        finished_evt = asyncio.Event()
        finished_payload: dict = {}

        try:
            entry = self._resolve_worker_entry()
            argv = entry + [
                "--cmd", "measure",
                "--ch", str(self._chamber),
                "--usb", str(self._usb_index),
                "--duration", str(float(duration_sec)),
                "--integration_ms", str(int(integration_time_ms)),
                "--sample_interval_s", str(float(self._sample_interval_s)),
                "--avg_count", str(int(self._avg_count)),
                "--out_dir", str(self._local_dir),
            ]
            if self._dll_path:
                argv += ["--dll_path", str(self._dll_path)]

            await self._status(
                f"[OES] WORKER measure start: ch={self._chamber} usb={self._usb_index} "
                f"dur={float(duration_sec):.1f}s integ={int(integration_time_ms)}ms out_dir={self._local_dir}"
            )

            proc = await asyncio.create_subprocess_exec(
                *argv,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                creationflags=self._win_creationflags(),
            )
            self._proc = proc

            self._stdout_task = asyncio.create_task(
                self._read_stdout(proc, started_evt, finished_evt, finished_payload),
                name=f"OESWorkerStdout_CH{self._chamber}",
            )
            self._stderr_task = asyncio.create_task(
                self._read_stderr(proc),
                name=f"OESWorkerStderr_CH{self._chamber}",
            )

            # started 대기(최대 30초)
            t0 = time.time()
            while not started_evt.is_set() and proc.returncode is None and (time.time() - t0) < 30.0:
                if self._stop_requested:
                    break
                await asyncio.sleep(0.1)

            if self._out_csv:
                self._tail_task = asyncio.create_task(
                    self._tail_csv(self._out_csv),
                    name=f"OESWorkerTail_CH{self._chamber}",
                )
            else:
                await self._status("[OES] started 메시지를 받지 못했습니다(초기화/실행 실패 가능)")

            # 종료 대기: (duration + grace) 또는 finished JSON 중 먼저
            wait_timeout = max(20.0, float(duration_sec) + float(self._grace_s))
            done, _ = await asyncio.wait(
                [asyncio.create_task(proc.wait()), asyncio.create_task(finished_evt.wait())],
                timeout=wait_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                await self._status("[OES] WORKER timeout → 강제 종료")
                await self._terminate_proc()
            else:
                # proc가 끝나지 않았는데 finished만 온 경우 보정
                if proc.returncode is None:
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(proc.wait(), timeout=10.0)

        except Exception as e:
            await self._status(f"[OES] WORKER 실행 예외: {type(e).__name__}: {e}")
            if self._debug:
                traceback.print_exc()
        finally:
            # tail 정리
            if self._tail_task:
                with contextlib.suppress(Exception):
                    self._tail_task.cancel()
                    await self._tail_task
                self._tail_task = None

            # stdout/stderr 정리
            await self._shutdown_reader_tasks()

        # finished 결과 판단
        ok = bool(finished_payload.get("ok", False)) if finished_payload else False

        if self._stop_requested:
            ok = True
            await self._status(f"[OES] stop requested: {self._stop_reason}")

        # 종료 후 NAS 이동
        final_path = None
        if self._out_csv and self._out_csv.is_file():
            moved = await self._move_to_nas(self._out_csv)
            if moved:
                final_path = moved
                await self._status(f"[OES] NAS 이동 완료: {moved}")
            else:
                final_path = self._out_csv
                await self._status(f"[OES] NAS 이동 실패 → 로컬 유지: {self._out_csv}")

        self._last_save_path = final_path
        self._saved_once = True if final_path else False

        if not ok and finished_payload:
            await self._status(f"[OES] WORKER finished FAIL: {finished_payload}")

        self._ev_nowait(OESEvent(kind="finished", success=bool(ok), message="측정 완료" if ok else "측정 실패"))
        self.is_running = False

    async def stop_measurement(self):
        if not self.is_running:
            return
        self._stop_requested = True
        self._stop_reason = "사용자 stop"
        await self._status("[OES] stop_measurement 요청")
        await self._terminate_proc()

    async def cleanup(self):
        if self.is_running:
            self._stop_requested = True
            self._stop_reason = "사용자 중단(cleanup)"
            await self._status("[OES] cleanup → 워커 종료 요청")
            await self._terminate_proc()

    async def drain_events(self):
        """(선택) 런 시작 전 잔여 이벤트 드레인 용도"""
        drained = 0
        while True:
            try:
                self._ev_q.get_nowait()
                drained += 1
            except Exception:
                break
        if drained:
            await self._status(f"[OES] drain_events: {drained} events dropped")

    async def events(self) -> AsyncGenerator[OESEvent, None]:
        while True:
            ev = await self._ev_q.get()
            yield ev

    def shutdown_executor(self):
        # 워커 클라이언트는 executor를 쓰지 않음
        return

    # ─────────────────────────────
    # Internal
    # ─────────────────────────────

    def _reset_for_new_run(self):
        self._stop_requested = False
        self._stop_reason = ""
        self._saved_once = False
        self._last_save_path = None
        self._out_csv = None
        self._tail_x = None

    async def _status(self, msg: str):
        try:
            get_app_logger().info("%s", msg)
        except Exception:
            pass
        await self._ev_q.put(OESEvent(kind="status", message=msg))

    def _ev_nowait(self, ev: OESEvent):
        try:
            self._ev_q.put_nowait(ev)
        except Exception:
            pass

    def _win_creationflags(self) -> int:
        if os.name != "nt":
            return 0
        return int(getattr(subprocess, "CREATE_NO_WINDOW", 0))

    def _resolve_worker_entry(self) -> List[str]:
        """
        워커 실행 엔트리(argv prefix) 탐색 우선순위
        1) self._worker_path
        2) 환경변수 OES_WORKER_PATH
        3) (frozen) exe_dir/apps/oes_service/oes_api.exe (+ _internal 변형도 체크)
        4) (source) 프로젝트 루트/apps/oes_service/oes_api.exe 또는 oes_api.py
        """
        cand = self._worker_path or os.getenv("OES_WORKER_PATH")
        if cand:
            p = Path(cand)
            if p.suffix.lower() == ".py":
                if getattr(sys, "frozen", False):
                    raise FileNotFoundError(f"(frozen) 워커를 .py로 실행할 수 없습니다: {p}")
                return [sys.executable, str(p)]
            return [str(p)]

        # frozen 배포 폴더 기준
        if getattr(sys, "frozen", False):
            base = Path(sys.executable).resolve().parent
            for root in (base, base / "_internal"):
                for name in ("oes_api.exe", "oes_service.exe"):
                    exe = root / "apps" / "oes_service" / name
                    if exe.is_file():
                        return [str(exe)]

        # 소스 실행(프로젝트 기준)
        root = Path(__file__).resolve().parents[1]  # CH_1_2_program/
        for name in ("oes_api.exe", "oes_service.exe"):
            exe = root / "apps" / "oes_service" / name
            if exe.is_file():
                return [str(exe)]

        py1 = root / "apps" / "oes_service" / "oes_api.py"
        if py1.is_file() and not getattr(sys, "frozen", False):
            return [sys.executable, str(py1)]

        raise FileNotFoundError("OES worker entry not found. 기대: apps/oes_service/oes_api.exe")

    async def _run_capture(self, argv: List[str], timeout_s: float) -> tuple[int, str, str]:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            creationflags=self._win_creationflags(),
        )
        try:
            out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
        except asyncio.TimeoutError:
            with contextlib.suppress(Exception):
                proc.kill()
            raise
        out = (out_b or b"").decode("utf-8", errors="ignore")
        err = (err_b or b"").decode("utf-8", errors="ignore")
        return int(proc.returncode or 0), out, err

    def _parse_last_json(self, out_text: str) -> Optional[dict]:
        """stdout에서 JSON 한 줄(마지막 유효 JSON)을 찾아 파싱"""
        for line in reversed(out_text.splitlines()):
            line = line.strip()
            if not line:
                continue
            try:
                return json.loads(line)
            except Exception:
                continue
        return None

    async def _read_stdout(
        self,
        proc: asyncio.subprocess.Process,
        started_evt: asyncio.Event,
        finished_evt: asyncio.Event,
        finished_payload: dict,
    ) -> None:
        assert proc.stdout is not None
        try:
            while True:
                line_b = await proc.stdout.readline()
                if not line_b:
                    return
                line = line_b.decode("utf-8", errors="ignore").strip()
                if not line:
                    continue
                try:
                    j = json.loads(line)
                except Exception:
                    continue

                kind = j.get("kind")
                if kind == "started":
                    out_csv = j.get("out_csv")
                    if out_csv:
                        self._out_csv = Path(str(out_csv))
                    self.sChannel = int(j.get("resolved_usb", self._usb_index))
                    started_evt.set()
                elif kind == "finished":
                    finished_payload.clear()
                    finished_payload.update(j)
                    finished_evt.set()
                elif kind == "status":
                    await self._status(str(j.get("message", j)))
        except asyncio.CancelledError:
            return
        except Exception as e:
            await self._status(f"[OES] stdout reader 예외: {type(e).__name__}: {e}")

    async def _read_stderr(self, proc: asyncio.subprocess.Process) -> None:
        """stderr는 버퍼가 차서 워커가 멈추는 걸 막기 위해 계속 소비(마지막 일부만 로그)"""
        assert proc.stderr is not None
        buf: List[str] = []
        try:
            while True:
                line_b = await proc.stderr.readline()
                if not line_b:
                    return
                line = line_b.decode("utf-8", errors="ignore").rstrip()
                if not line:
                    continue
                buf.append(line)
                if len(buf) > 50:
                    buf.pop(0)
        except asyncio.CancelledError:
            return
        except Exception:
            return
        finally:
            if buf:
                with contextlib.suppress(Exception):
                    get_app_logger().warning("[OES][WORKER][STDERR]\n%s", "\n".join(buf[-20:]))

    async def _shutdown_reader_tasks(self):
        for tname in ("_stdout_task", "_stderr_task"):
            t: Optional[asyncio.Task] = getattr(self, tname)
            if not t:
                continue
            setattr(self, tname, None)
            with contextlib.suppress(Exception):
                t.cancel()
                await t

    async def _terminate_proc(self):
        proc = self._proc
        if not proc:
            return
        if proc.returncode is not None:
            return

        with contextlib.suppress(Exception):
            proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except Exception:
            with contextlib.suppress(Exception):
                proc.kill()
        finally:
            self._proc = None

    async def _tail_csv(self, csv_path: Path) -> None:
        """
        로컬 CSV를 tail 하여 "data" 이벤트를 발생.
        - 헤더: Time, x1, x2, ...
        - 데이터: time_str, y1, y2, ...
        """
        t0 = time.time()
        while not csv_path.is_file():
            if not self.is_running:
                return
            if time.time() - t0 > 15.0:
                await self._status(f"[OES][TAIL] CSV 생성 timeout: {csv_path}")
                return
            await asyncio.sleep(0.1)

        try:
            with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
                header = f.readline()
                if not header:
                    await self._status(f"[OES][TAIL] 헤더 읽기 실패: {csv_path}")
                    return

                try:
                    rest = header.strip().split(",", 1)[1]
                    x_arr = np.fromstring(rest, sep=",")
                    self._tail_x = x_arr.astype(float).tolist()
                except Exception:
                    self._tail_x = None

                while self.is_running:
                    if self._stop_requested:
                        return

                    line = f.readline()
                    if not line:
                        await asyncio.sleep(self._tail_poll_s)
                        continue
                    if "," not in line:
                        continue

                    line = line.strip()
                    if not line:
                        continue

                    try:
                        _, rest = line.split(",", 1)
                        y_arr = np.fromstring(rest, sep=",")
                        y_list = y_arr.astype(float).tolist()
                        if self._tail_x is None:
                            self._tail_x = list(map(float, range(len(y_list))))
                        self._ev_nowait(OESEvent(kind="data", x=self._tail_x, y=y_list))
                    except Exception:
                        continue

        except asyncio.CancelledError:
            return
        except Exception as e:
            await self._status(f"[OES][TAIL] 예외: {type(e).__name__}: {e}")

    async def _move_to_nas(self, local_csv: Path) -> Optional[Path]:
        """
        NAS로 '이동' (안전한 copy+검증+rename 후 로컬 삭제).
        NAS 실패 시 None 반환(로컬 유지).
        """
        nas_dir = self._nas_dir
        dest = nas_dir / local_csv.name

        def _impl() -> Optional[Path]:
            try:
                nas_dir.mkdir(parents=True, exist_ok=True)

                tmp = dest.with_suffix(dest.suffix + ".tmp")
                if tmp.exists():
                    try:
                        tmp.unlink()
                    except Exception:
                        pass

                shutil.copy2(local_csv, tmp)

                if not tmp.is_file():
                    return None

                s1 = local_csv.stat().st_size if local_csv.is_file() else -1
                s2 = tmp.stat().st_size if tmp.is_file() else -2
                if s1 <= 0 or s1 != s2:
                    try:
                        tmp.unlink()
                    except Exception:
                        pass
                    return None

                try:
                    if dest.exists():
                        dest.unlink()
                except Exception:
                    pass
                tmp.replace(dest)

                try:
                    local_csv.unlink()
                except Exception:
                    pass

                return dest
            except Exception:
                return None

        return await asyncio.to_thread(_impl)

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
        default_timeout_s: float = 60.0,
    ):
        self.ch = int(ch)
        self.worker_path = worker_path
        self.logger = logger
        self.default_timeout_s = float(default_timeout_s)

        self._q: asyncio.Queue[RGAEvent] = asyncio.Queue()
        self._connected = True  # chamber_runtime에서 “있으면 pump” 정도로만 사용

    @staticmethod
    def _main_exe_dir() -> Path:
        if getattr(sys, "frozen", False):
            return Path(sys.executable).resolve().parent
        return Path(sys.argv[0]).resolve().parent

    def _resolve_worker_cmd(self) -> Tuple[str, ...]:
        # 0) 사용자가 명시적으로 지정한 경로가 있으면 최우선(기존 유지)
        if self.worker_path is not None:
            p = Path(self.worker_path).expanduser()
            if p.suffix.lower() == ".exe":
                if not p.exists():
                    raise FileNotFoundError(f"rga_worker.exe not found: {p}")
                return (str(p),)
            if not p.exists():
                raise FileNotFoundError(f"rga worker script not found: {p}")
            return (sys.executable, str(p))

        # ✅ 하드코딩 요구사항:
        # 메인 공정 프로그램.exe 폴더 기준으로만 실행
        base = self._main_exe_dir()
        exe = base / "apps" / "rga_service" / "rga_worker.exe"

        if not exe.exists():
            raise FileNotFoundError(
                "rga_worker.exe not found (hardcoded)\n"
                f" expected={exe}\n"
                f" base_dir={base}\n"
                f" cwd={Path.cwd()}"
            )

        return (str(exe),)

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

        ✅ 변경: 총 3회 재시도
        - 1~2번째 실패: status만 남기고 재시도 (failed/finished 안 보냄)
        - 3번째 실패: failed + finished를 보내서 런타임이 다음 단계로 진행하게 함
        """
        if timeout_s is None:
            timeout_s = self.default_timeout_s

        max_attempts = 3
        retry_delay_s = 1.0  # 실패 후 재시도 전에 잠깐 쉬기(너무 공격적으로 재시도하면 PC/프로세스가 더 꼬일 수 있음)

        creationflags = 0
        if sys.platform.startswith("win"):
            creationflags = subprocess.CREATE_NO_WINDOW

        def _tail(s: str, n: int = 4000) -> str:
            return (s or "")[-n:]

        last_fail: Dict[str, Any] = {"message": "unknown"}

        for attempt in range(1, max_attempts + 1):
            fail: Optional[Dict[str, Any]] = None

            # 1) worker 커맨드 해석
            try:
                cmd_base = self._resolve_worker_cmd()
            except Exception as e:
                fail = {
                    "message": f"RGA worker resolve failed: {e!r}",
                    "attempt": attempt,
                    "attempts": max_attempts,
                }

            if fail is None:
                cmd = [*cmd_base, "--ch", str(self.ch), "--timeout", str(timeout_s)]
                await self._q.put(
                    RGAEvent("status", {"message": f"RGA worker attempt {attempt}/{max_attempts} start: {' '.join(cmd)}"})
                )

                proc = None
                out = ""
                err = ""
                returncode = None

                # 2) 프로세스 실행
                try:
                    proc = await asyncio.create_subprocess_exec(
                        *cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                        stdin=asyncio.subprocess.DEVNULL,
                        creationflags=creationflags,
                    )
                except FileNotFoundError as e:
                    fail = {
                        "message": f"RGA worker exec failed: {e!r}",
                        "attempt": attempt,
                        "attempts": max_attempts,
                        "cmd": cmd,
                    }
                except Exception as e:
                    fail = {
                        "message": f"RGA worker exec unexpected error: {e!r}",
                        "attempt": attempt,
                        "attempts": max_attempts,
                        "cmd": cmd,
                    }

                # 3) stdout/stderr 수집(+timeout)
                if fail is None:
                    try:
                        out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
                        returncode = proc.returncode
                        out = (out_b or b"").decode("utf-8", errors="replace")
                        err = (err_b or b"").decode("utf-8", errors="replace")
                    except asyncio.TimeoutError:
                        with contextlib.suppress(Exception):
                            proc.kill()
                        with contextlib.suppress(Exception):
                            await asyncio.wait_for(proc.wait(), timeout=2.0)

                        fail = {
                            "message": f"RGA worker timeout ({timeout_s}s)",
                            "attempt": attempt,
                            "attempts": max_attempts,
                            "cmd": cmd,
                        }
                    except Exception as e:
                        fail = {
                            "message": f"RGA worker communicate error: {e!r}",
                            "attempt": attempt,
                            "attempts": max_attempts,
                            "cmd": cmd,
                        }

                # 4) JSON 파싱 / ok 판정
                if fail is None:
                    payload = self._parse_json_from_stdout(out)

                    if not payload or not isinstance(payload, dict):
                        msg = "RGA worker stdout JSON parse failed"
                        fail = {
                            "message": msg,
                            "attempt": attempt,
                            "attempts": max_attempts,
                            "cmd": cmd,
                            "returncode": returncode,
                            "stdout": _tail(out),
                            "stderr": _tail(err),
                        }
                    elif not payload.get("ok", False):
                        msg = payload.get("error") or payload.get("message") or "RGA worker failed"
                        fail = {
                            "message": str(msg),
                            "attempt": attempt,
                            "attempts": max_attempts,
                            "cmd": cmd,
                            "returncode": returncode,
                            "meta": payload,
                            "stdout": _tail(out),
                            "stderr": _tail(err),
                        }
                    else:
                        # ✅ 성공
                        mass_axis = payload.get("mass_axis") or []
                        pressures = payload.get("pressures") or []

                        await self._q.put(RGAEvent("data", {"mass_axis": mass_axis, "pressures": pressures, "meta": payload}))
                        await self._q.put(RGAEvent("finished", {"message": f"RGA scan finished (attempt {attempt}/{max_attempts})"}))
                        return

            # ===== 실패 처리 =====
            last_fail = fail or {"message": "unknown", "attempt": attempt, "attempts": max_attempts}

            # ✅ 실패 원인은 로그(파일)에도 남김
            if self.logger:
                try:
                    self.logger.error("[RGA] attempt %s/%s failed: %s | detail=%s",
                                    attempt, max_attempts, last_fail.get("message"), {k: last_fail.get(k) for k in ("cmd", "returncode")})
                except Exception:
                    pass

            if attempt < max_attempts:
                # 1~2번째 실패: status만 남기고 재시도 (중요: failed/finished 내보내면 공정이 다음 단계로 넘어가버림)
                await self._q.put(
                    RGAEvent("status", {"message": f"[RGA] attempt {attempt}/{max_attempts} 실패 → 재시도({retry_delay_s}s): {last_fail.get('message')}"})
                )
                if retry_delay_s > 0:
                    await asyncio.sleep(retry_delay_s)
                continue

            # ✅ 3번째(최종) 실패: 여기서만 failed + finished → 런타임이 다음 단계로 진행
            await self._q.put(RGAEvent("failed", last_fail))
            await self._q.put(RGAEvent("finished", {"message": f"RGA scan finished (failed after {max_attempts})"}))
            return

    async def events(self) -> AsyncIterator[RGAEvent]:
        # ✅ cleanup 때문에 영구 종료되면 다음 공정에서 RGA finished 신호가 안 올라와서 무한대기함
        while True:
            ev = await self._q.get()
            yield ev

    async def cleanup(self) -> None:
        # ✅ 여기서 스트림을 닫지 않는다(외부 pump task cancel로 종료)
        return

r"""OES 링크 — one-shot 워커(측정·init 단독)와 빠른 정리. v1 device/oes.py 의 one-shot 경로를 옮긴 것.

- init 단독(OES_DAEMON 꺼짐): '--cmd init --ch --usb', kind=init 줄을 timeout 안에, 뒤에 3 s 안에 안 끝나면 kill(5 s). 결과 캐시.
- 측정: '--cmd measure --ch --usb --duration --integration_ms --sample_interval_s --avg_count --out_csv',
  env OES_INIT_TIMEOUT_S = max(5, min(25, duration+10)), 종료 대기 max(10, duration+60) s 넘으면 kill.
  판정: 워커 finished JSON 의 ok·rows·elapsed·error 우선, 없으면 rc==0 이고 CSV 가 있고 크기 > 0.
  실패 문구 'OES 측정 실패(rc=N)' + 워커 error + stderr 마지막 50줄.
- STOP(one-shot): 플래그 2개 → wait 면 종료 대기 → terminate(5 s) → kill(5 s) → 플래그 정리.
- cleanup_quick: tail 중지 → daemon kill → init 프로세스 terminate(2 s)→kill(2 s) → 플래그 삭제 → 측정 워커 terminate→kill → 상태 초기화.
v1 근거: device/oes.py:258-440(init), 834-1094(측정), 1109-1199(STOP), 1240-1326(정리).
"""
from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any, Dict, List, Optional

from v2.ch12.drivers import worker_proc as wp


class OneShotMixin:
    # OesLink 가 가진 속성: clock·ch·usb·_proc·_init_proc·_tail·_out_csv·_rows_seen·_stop_requested·is_running·...

    # ───────── 메시지 ─────────
    def _on_oneshot_msg(self, obj: Dict[str, Any]) -> None:
        """v1 _watch_worker_stdout — status·started·finished/fatal 만(그 밖은 버림)."""
        k = obj.get("kind")
        if k == "status":
            msg = obj.get("message", "")
            if msg:
                self._status(f"[OES] {msg}")
        elif k == "started":
            self._status(f"[OES] worker started out_csv={obj.get('out_csv')} cols={obj.get('cols')} usb={obj.get('resolved_usb')}")
        elif k in ("finished", "fatal"):
            self._worker_finished = obj
            if bool(obj.get("ok", False)):
                self._status("[OES] worker finished OK")
            else:
                self._status(f"[OES] worker finished FAIL: {obj.get('error', '')}")

    def _on_init_msg(self, obj: Dict[str, Any]) -> None:
        k = obj.get("kind")
        if k == "hb":
            return
        if k == "_raw":
            self._status(f"[OES][worker] {obj.get('text')}")
        elif k == "status":
            self._status(str(obj.get("message", "")))
        elif k == "init":
            with self.clock.cond:
                self._init_obj = obj
                self.clock.cond.notify_all()
        else:
            self._status(f"[OES][worker] {obj}")

    # ───────── init 단독 ─────────
    def _init_oneshot(self, timeout_s: float, force: bool) -> bool:
        if self._init_done and self._init_ok and not force:
            return True
        base = self._worker_argv()
        self._init_done = self._init_ok = False
        self.init_error = None
        if base is None:
            self.init_error = "worker exe not found"
            self._status(f"[OES] init EXC: {self.init_error}")
            self._init_done = True
            return False
        argv = [*base, "--cmd", "init", "--ch", str(self.ch), "--usb", str(self.usb), "--dll_path", self._dll_path(),
                *self._v2_args(next(self._ids))]
        wt = max(5.0, float(timeout_s) - 2.0)
        self._status(f"[OES] init spawn ch={self.ch} usb={self.usb} parent_timeout={timeout_s}s worker_timeout={wt}s cmd={argv}")
        self._init_obj = None
        try:
            h = self._spawn(argv, {"OES_INIT_TIMEOUT_S": str(wt)}, self._on_init_msg, "init")
        except OSError as e:
            self.init_error = f"{type(e).__name__}: {e}"
            self._status(f"[OES] init EXC: {self.init_error}")
            self._init_done = True
            return False
        self._init_proc = h
        try:
            got = self.clock.wait_until(self.clock.monotonic() + float(timeout_s),
                                        lambda: self._init_obj is not None or h.eof)
            tail = list(h.stderr_tail)
            if not got:
                h.kill()
                h.wait(5.0)
                self.init_error = f"init timeout after {timeout_s}s" + (" | stderr_tail=" + " / ".join(tail[-5:]) if tail else "")
                self._status(f"[OES] init TIMEOUT: {self.init_error}")
                self._init_done = True
                return False
            if not h.wait(3.0):
                h.kill()
                h.wait(5.0)
            tail = list(h.stderr_tail)
            obj = self._init_obj
            if not obj:
                self.init_error = f"init JSON not received (rc={h.returncode})" + (" | stderr_tail=" + " / ".join(tail[-5:]) if tail else "")
                self._status(f"[OES] init FAIL: {self.init_error}")
                self._init_done = True
                return False
            if bool(obj.get("ok", False)):
                self._init_ok = True
                self._status(f"[OES] init OK: {obj}")
                self._init_done = True
                return True
            self.init_error = f"{obj.get('error') or 'initialize_device failed'} | scan={obj.get('scan') or {}}" + \
                (" | stderr_tail=" + " / ".join(tail[-5:]) if tail else "")
            self._status(f"[OES] init FAIL: {self.init_error}")
            self._init_done = True
            return False
        finally:
            h.join(1.0)
            if self._init_proc is h:
                self._init_proc = None

    # ───────── 측정 ─────────
    def _preempt_oneshot(self) -> None:
        """v1: 이전 워커가 정리 중·유령이면 stop 플래그 → OES_PREEMPT_WAIT_S(5 s) 기다려 안 끝나면 cleanup_quick(별도 스레드)."""
        p = self._proc
        self._status("[OES] 이전 워커가 종료/정리 중 → stop 요청 후 짧게 대기")
        self._stop_oneshot(False, 0.0)
        wait_s = max(0.2, self._f("OES_PREEMPT_WAIT_S", 5.0))

        def run() -> None:
            if p is not None and not p.wait(wait_s):
                self._status("[OES] 이전 워커 종료 지연 → cleanup_quick로 강제 종료")
                self.cleanup_quick()
        self._threads.append(self.clock.start_thread(run, f"oes{self.ch}-preempt"))

    def _run_oneshot(self, duration_s: float, integration_ms: int) -> "Any":
        from v2.ch12.drivers.oes_link import AVG_COUNT, SAMPLE_INTERVAL_S, OesResult
        if self.is_running and (self._proc is None or not self._proc.alive()):
            self._status("[OES] is_running=True 이지만 워커 없음/종료됨 → 유령상태 복구 후 재시작")
            self.is_running = False
            self._cleanup_proc_tasks()
        base = self._worker_argv()
        if self._proc is None or not self._proc.alive():
            self._clear_local_flags()        # stop flag 삭제는 워커가 없을 때만
        out_csv = self._new_measure()
        if base is None:
            exe = wp.resolve_path(str(self._c("OES_WORKER_EXE", "")))
            self._status(f"[OES] 워커 exe를 찾지 못함 → 측정 스킵\n expected={exe}\n base_dir={wp.REPO_ROOT}\n cwd={Path.cwd()}")
            self.is_running = False
            return OesResult(True, False, "OES 측정 실패(워커 exe 없음)", str(out_csv), 0, 0.0, f"worker exe not found: {exe}")
        cid = next(self._ids)
        argv = [*base, "--cmd", "measure", "--ch", str(self.ch), "--usb", str(self.usb),
                "--duration", str(float(duration_s)), "--integration_ms", str(int(integration_ms)),
                "--sample_interval_s", str(float(SAMPLE_INTERVAL_S)), "--avg_count", str(int(AVG_COUNT)),
                "--out_csv", str(out_csv), "--dll_path", self._dll_path(), *self._v2_args(cid)]
        t0 = self.clock.monotonic()
        self.is_running = True
        try:
            self._status(f"[OES] measure spawn\n  cmd={argv}\n  creationflags={self._creationflags()}\n  out_csv={out_csv}\n  cwd={self._home()}")
            wt = max(5.0, min(25.0, float(duration_s) + 10.0))
            h = self._spawn(argv, {"PYTHONUNBUFFERED": "1", "OES_INIT_TIMEOUT_S": str(wt)}, self._on_oneshot_msg, "measure")
            self._proc = h
            self._start_tail(out_csv)
            self._status(f"[OES] 측정 시작: {duration_s / 60:.1f}분, out={out_csv}")
            if not h.wait(self._measure_timeout(duration_s)):
                self._status("[OES] 워커 timeout → 강제 종료")
                h.kill()
                h.wait(5.0)
            rc = int(h.returncode or 0)
            self.clock.wait_until(self.clock.monotonic() + 2.0, lambda: h.eof)
            self._stop_tail()
            elapsed = self.clock.monotonic() - t0
            wf = self._worker_finished if isinstance(self._worker_finished, dict) else None
            if wf is not None and wf.get("kind") in ("finished", "fatal"):
                ok = bool(wf.get("ok", False))
            else:
                wf = None
                ok = rc == 0 and out_csv.exists() and out_csv.stat().st_size > 0
            rows = wf.get("rows") if wf else None
            el = wf.get("elapsed_s") if wf else None
            final_rows = int(rows) if isinstance(rows, (int, float)) else int(self._rows_seen)
            final_elapsed = float(el) if isinstance(el, (int, float)) else float(elapsed)
            src = self._worker_finished if isinstance(self._worker_finished, dict) else {}
            nas_ok, nas_csv, nas_error = bool(src.get("nas_ok", False)), src.get("nas_csv"), src.get("nas_error")
            final_path, msg = out_csv, "OES 측정 완료"
            if ok and nas_ok and nas_csv:
                final_path, msg = Path(nas_csv), "OES 측정 완료(NAS 저장)"
            elif ok and not nas_ok:
                msg = f"OES 측정 완료(로컬 저장, NAS 실패: {nas_error})" if nas_error else "OES 측정 완료(로컬 저장)"
            if ok and nas_ok and out_csv.exists() and not bool(src.get("local_deleted", False)):
                with contextlib.suppress(OSError):
                    out_csv.unlink()
            err = None
            if not ok:
                parts: List[str] = []
                if wf and wf.get("error"):
                    parts.append(str(wf.get("error")))
                tail = list(h.stderr_tail)
                if tail:
                    parts.append("stderr_tail:\n" + "\n".join(tail[-50:]))
                err = "\n".join(parts or [f"worker rc={rc}"])
            return OesResult(True, bool(ok), msg if ok else f"OES 측정 실패(rc={rc})", str(final_path),
                             final_rows, final_elapsed, err)
        except Exception as e:
            self._status(f"[OES] 예외: {type(e).__name__}: {e}")
            return OesResult(True, False, "OES 예외", str(out_csv), self._rows_seen, self.clock.monotonic() - t0,
                             f"{type(e).__name__}: {e}")
        finally:
            self._stop_tail()
            self._cleanup_proc_tasks()
            self.is_running = False

    def _cleanup_proc_tasks(self) -> None:
        """v1 _cleanup_proc_tasks — STOP 요청이 있었으면 20 s 정상 종료 대기, 그 뒤 terminate(5 s) → kill(5 s)."""
        p = self._proc
        if p is None:
            return
        if p.alive() and self._stop_requested:
            p.wait(20.0)
        if p.alive():
            p.terminate()
            p.wait(5.0)
        if p.alive():
            p.kill()
            p.wait(5.0)
        p.join(1.0)
        if self._proc is p:
            self._proc = None

    def _stop_oneshot(self, wait: bool, timeout_s: float) -> None:
        p = self._proc
        if p is None or not p.alive():
            self._stop_tail()
            self._stop_requested = False
            if p is not None and not self.is_running:
                self._cleanup_proc_tasks()
            return
        self._stop_requested = True
        self._status(f"[OES] stop_measurement → request stop flag (pid={p.pid})")
        self._stop_tail()
        usb_flag, csv_flag = self._local_stop_flags()
        self._write_flags(usb_flag, csv_flag)
        if not wait:
            self._status("[OES] stop flag set (detach) → 워커 종료는 백그라운드에서 진행")
            return
        if not p.wait(float(timeout_s)):
            self._status("[OES] stop: graceful timeout → terminate/kill fallback")
            p.terminate()
            p.wait(5.0)
            if p.alive():
                p.kill()
                p.wait(5.0)
        self.clock.wait_until(self.clock.monotonic() + 2.0, lambda: p.eof)
        for f in (usb_flag, csv_flag):
            if f is not None:
                with contextlib.suppress(OSError):
                    f.unlink()
        self._clear_local_flags()
        if not self.is_running:
            self._cleanup_proc_tasks()

    def cleanup_quick(self) -> None:
        """v1 cleanup_quick — 다음 측정을 막지 않게 빠른 정리(막힘 최대 수 초)."""
        self._stop_tail()
        if self.daemon_enabled:
            with self._daemon_lock:
                self._shutdown_daemon(graceful=False)
        ip = self._init_proc
        if ip is not None and ip.alive():
            ip.terminate()
            ip.wait(2.0)
            if ip.alive():
                ip.kill()
                ip.wait(2.0)
        if self._init_proc is ip:
            self._init_proc = None
        usb_flag, csv_flag = self._local_stop_flags()
        for f in (usb_flag, csv_flag):
            if f is not None:
                with contextlib.suppress(OSError):
                    f.unlink()
        self._clear_local_flags()
        p = self._proc
        if p is not None and p.alive():
            p.terminate()
            p.wait(2.0)
            if p.alive():
                p.kill()
                p.wait(2.0)
        if p is not None:
            p.join(1.0)
        if self._proc is p:
            self._proc = None
        self._stop_requested = False
        self.is_running = False
        self._init_done = self._init_ok = False
        self.init_error = None

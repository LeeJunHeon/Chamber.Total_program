r"""RGA 링크(메인 쪽) — 별도 프로세스 RGA 워커(v2/ch12/workers/rga_worker, srsinst 는 워커 안에서만)를 부른다.

v1 device/rga.py(RGAWorkerClient) 동작을 스레드로 다시 쓴 것(v1 import 없음):
- 스캔 1회 = 최대 RGA_MAX_ATTEMPTS(3)회 시도, 시도마다 RGA_WORKER_TIMEOUT_S(60 s), 실패 사이 RGA_RETRY_DELAY_S(1 s).
  1~2번째 실패는 status '[RGA] attempt i/n 실패 → 재시도(1.0s): ..' 만, 마지막 실패에만 failed + finished.
  시작 때 status '[RGA] 스캔 시작 — 예산 3회 x 60s(+대기 1s) = 총 182s'. 공정 스텝 예산은 step_timeout_ms(cfg).
- 응답: ok=False 면 error/message 로 실패, ok=True 면 data(mass_axis, pressures, meta) + finished.
  worker_version 없음 → '구버전' status, csv_ok=False → 'CSV 저장 실패(primary=..): .. → fallback=..', csv_fallback_used →
  '정본 저장 실패 → 로컬 폴백에 저장'(측정 성공 유지 — CSV 실패는 공정 실패가 아니다).
- 시간 초과·통신 예외·취소면 워커를 바로 kill(2 s), cleanup 은 terminate → 2 s → kill → 2 s.
- CSV 인자: 정본 CHn_RGA_CSV_PATH(G:\공유 드라이브\VanaM_Sputter\RGA\Ch.n\RGA_spectrums.csv — v1 그대로, D171·D07 확인 필요),
  폴백 LOCAL_FALLBACK_RGA_DIR\Ch.n\RGA_spectrums.csv.
v2 연결부: 워커는 상주(JSONL) — v1 은 시도마다 '--ch --timeout --csv --csv_fallback' 로 새로 띄웠다. v2 는 같은 이름의 인자를
  scan 명령 줄로 보내고(id 로 응답 매칭), 시간 초과·죽음이면 kill 하고 다음 시도에 새로 띄운다(v1 과 같은 결과).
  장비 계정은 설정의 비밀값 RGA_NET({"ch1": {"user", "password"}, ...})에서 명령 줄로만 넘긴다(명령줄·로그에 남기지 않음).
1초 행(블록 "<ch>.rga")에는 상태만: running(워커 살아 있음)·measuring(스캔 중)·sample_age_s(마지막 성공 스캔 나이).
"""
from __future__ import annotations

import itertools
import json
import os
import subprocess
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from v2.common.snapshot import DeviceBlock, FieldDef, FType
from v2.ch12.drivers import worker_proc as wp

MODULE = "v2.ch12.workers.rga_worker"
DEFAULT_CSV = r"G:\공유 드라이브\VanaM_Sputter\RGA\Ch.{n}\RGA_spectrums.csv"

BUDGET = dict(queries_per_tick=0, bytes_tx=0, bytes_rx=0, min_gap_ms=0, fits_1s=True, alternate=None,
              note="장비 통신 없음 — 1초 행은 링크 상태만 메모리에서. RGA 스캔은 공정 스텝(RGA_SCAN)에서만 워커가 수행(최대 60 s × 3)")


def blocks(chamber: str) -> List[DeviceBlock]:
    return [DeviceBlock(f"{chamber}.rga", f"{chamber.upper()} RGA", (
        FieldDef("running", FType.BOOL, "", "RGA 워커 실행 중", source="링크(프로세스 살아 있음)"),
        FieldDef("measuring", FType.BOOL, "", "RGA 스캔 중", source="링크"),
        FieldDef("sample_age_s", FType.FLOAT, "s", "마지막 스캔 나이", fmt=".0f", source="마지막 성공 스캔"),
    ), desc="RGA(SRS RGA100, 별도 프로세스 워커 — 값은 RGA CSV 에)")]


def step_timeout_ms(cfg: Dict[str, Any]) -> int:
    """v1 lib/config_common.py rga_step_timeout_ms = n×t×1000 + (n−1)×d×1000 + 5000."""
    n = max(1, int(cfg.get("RGA_MAX_ATTEMPTS", 3)))
    t = float(cfg.get("RGA_WORKER_TIMEOUT_S", 60.0))
    d = max(0.0, float(cfg.get("RGA_RETRY_DELAY_S", 1.0)))
    return int(n * t * 1000 + (n - 1) * d * 1000 + 5000)


@dataclass(frozen=True)
class RgaEvent:
    kind: str                 # status / data / finished / failed / rejected / worker_exit
    detail: str
    reason: Optional[str] = None
    data: Optional[dict] = None


@dataclass
class RgaResult:
    ok: bool
    message: str = ""
    mass_axis: list = field(default_factory=list)
    pressures: list = field(default_factory=list)
    meta: Optional[dict] = None
    fail: Optional[dict] = None
    reason: Optional[str] = None


def _tail(s: str, n: int = 4000) -> str:
    return (s or "")[-n:]


class RgaLink:
    NAME = "rga"

    def __init__(self, clock, worker, cfg: Dict[str, Any], writes_allowed: bool,
                 on_event: Optional[Callable[[RgaEvent], None]] = None, chamber: str = "ch1") -> None:
        self.clock, self.worker, self.cfg = clock, worker, cfg
        self.writes_allowed = writes_allowed
        self.on_event = on_event
        self.chamber = chamber
        self.ch = 1 if chamber == "ch1" else 2
        self.block = f"{chamber}.rga"
        self._ids = itertools.count(1)
        self._h: Optional[wp.ProcHandle] = None
        self._lock = threading.Lock()              # 스캔 한 번에 하나(v1 은 공정 스텝이 하나씩 부름)
        self._results: Dict[Any, dict] = {}
        self._raw: List[str] = []
        self.scanning = False
        self._cancel = False
        self._last_ok: Optional[float] = None
        self.sent: List[dict] = []                 # 보낸 줄(비밀번호 가림) — 골든 비교·진단
        self.spawned: List[List[str]] = []
        self._threads: List[threading.Thread] = []

    # ───────── 설정 ─────────
    def _c(self, key: str, default: Any) -> Any:
        return self.cfg.get(key, default)

    def csv_paths(self) -> tuple:
        """(정본, 폴백) — v1 _resolve_csv_paths."""
        primary = str(self._c(f"CH{self.ch}_RGA_CSV_PATH", DEFAULT_CSV.format(n=self.ch)) or "")
        d = str(self._c("LOCAL_FALLBACK_RGA_DIR", str(wp.REPO_ROOT / "Logs_LocalFallback" / "RGA")) or "")
        fallback = str(Path(d) / f"Ch.{self.ch}" / "RGA_spectrums.csv") if d else ""
        return primary, fallback

    def _creds(self) -> tuple:
        net = self._c("RGA_NET", {}) or {}
        if isinstance(net, str):         # 비밀값은 JSON 글자로 저장(params/rga.py)
            try:
                net = json.loads(net)
            except ValueError:
                net = {}
        ent = net.get(f"ch{self.ch}", net) if isinstance(net, dict) else {}
        if not isinstance(ent, dict):
            ent = {}
        return str(ent.get("user", "") or ""), str(ent.get("password", "") or "")

    def _base_argv(self) -> List[str]:
        exe = str(self._c("RGA_WORKER_EXE", "") or "")
        if exe and not wp.resolve_path(exe).is_file():
            raise FileNotFoundError(f"rga_worker.exe not found: {wp.resolve_path(exe)}")
        argv = wp.worker_command(exe, MODULE) + ["--hb_s", str(float(self._c("RGA_HEARTBEAT_S", 1.0)))]
        if bool(self._c("RGA_WORKER_FAKE", False)):
            argv.append("--fake")
        return argv

    # ───────── 이벤트·프로세스 ─────────
    def _emit(self, kind: str, detail: str, reason: Optional[str] = None, data: Optional[dict] = None) -> None:
        if self.on_event:
            try:
                self.on_event(RgaEvent(kind, detail, reason, data))
            except Exception:
                pass

    def _on_msg(self, obj: Dict[str, Any]) -> None:
        k = obj.get("kind")
        if k == "result":
            with self.clock.cond:
                self._results[obj.get("id")] = obj
                self.clock.cond.notify_all()
        elif k == "_raw":
            self._raw = (self._raw + [str(obj.get("text"))])[-50:]
        elif k == "status":
            self._emit("status", str(obj.get("message", "")))

    def _ensure_proc(self, base: List[str]) -> wp.ProcHandle:
        h = self._h
        if h is not None and h.alive():
            return h
        flags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0)) if os.name == "nt" else 0
        h = wp.ProcHandle(self.clock, f"rga{self.ch}", base, cwd=str(wp.REPO_ROOT), env=wp.worker_env(),
                          creationflags=flags, on_message=self._on_msg)
        self.spawned = (self.spawned + [list(base)])[-50:]
        h.start()
        self._h = h
        return h

    def _terminate_current(self, force_kill: bool) -> None:
        """v1 _terminate_current_proc — force 면 바로 kill(2 s), 아니면 terminate(2 s) → kill(2 s)."""
        h, self._h = self._h, None
        if h is None:
            return
        if h.alive():
            if not force_kill:
                h.terminate()
                h.wait(2.0)
            if h.alive():
                h.kill()
                h.wait(2.0)
        h.close_stdin()
        h.join(2.0)

    # ───────── 공개 API ─────────
    def scan(self, timeout_s: Optional[float] = None, on_done: Optional[Callable[[RgaResult], None]] = None,
             on_data: Optional[Callable[[list, list], None]] = None) -> None:
        """v1 scan_histogram_to_csv — 별도 스레드에서 돌고 끝나면 on_done(RgaResult). bench 면 BENCH_READ_ONLY."""
        if not self.writes_allowed:        # 필라멘트를 켜므로 쓰기로 본다
            self._emit("rejected", "[RGA] bench(읽기 전용) — 스캔 거부", "BENCH_READ_ONLY")
            if on_done:
                on_done(RgaResult(False, "BENCH_READ_ONLY", reason="BENCH_READ_ONLY"))
            return
        t = float(timeout_s if timeout_s is not None else self._c("RGA_WORKER_TIMEOUT_S", 60.0))

        def run() -> None:
            with self._lock:
                res = self._scan(t, on_data)
            if on_done:
                on_done(res)
        self._threads = [x for x in self._threads if x.is_alive()]
        self._threads.append(self.clock.start_thread(run, f"rga{self.ch}-scan"))

    def cancel(self) -> None:
        """v1 CancelledError 경로 — 진행 중 워커를 바로 kill, 남은 시도 없음."""
        self._cancel = True
        self.clock.notify()
        self._terminate_current(force_kill=True)

    def cleanup(self) -> None:
        """v1 cleanup — 스캔 중인 워커가 있으면 terminate → kill(쉬는 상주 워커는 그대로: 장비 연결을 잡고 있지 않음)."""
        if self.scanning:
            self._terminate_current(force_kill=False)

    def close(self) -> None:
        self._cancel = True
        self.clock.notify()
        h, self._h = self._h, None
        if h is not None:
            h.shutdown(True, {"v": 2, "id": next(self._ids), "cmd": "close"}, 2.0)
            h.join(2.0)
        for t in self._threads:
            if t is not threading.current_thread():
                t.join(5.0)
        self._threads = []

    def source_read(self, session, seq: int) -> Dict[str, Any]:
        b = self.block
        age = None if self._last_ok is None else round(self.clock.monotonic() - self._last_ok, 1)
        return {f"{b}.running": bool(self._h is not None and self._h.alive()), f"{b}.measuring": bool(self.scanning),
                f"{b}.sample_age_s": age}

    # ───────── 스캔(v1 device/rga.py:158-405) ─────────
    def _attempt(self, a: int, n: int, timeout_s: float, pri: str, fb: str) -> tuple:
        """한 번 시도 → (payload 또는 None, fail 또는 None)."""
        try:
            base = self._base_argv()
        except Exception as e:
            return None, {"message": f"RGA worker resolve failed: {e!r}", "attempt": a, "attempts": n}
        cmd = [*base, "--ch", str(self.ch), "--timeout", str(timeout_s)]      # 표시용(v1 명령줄과 같은 인자)
        if pri:
            cmd += ["--csv", pri]
        if fb:
            cmd += ["--csv_fallback", fb]
        self._emit("status", f"RGA worker attempt {a}/{n} start: {' '.join(cmd)}")
        try:
            h = self._ensure_proc(base)
        except Exception as e:
            return None, {"message": f"RGA worker exec failed: {e!r}", "attempt": a, "attempts": n, "cmd": cmd}
        cid = next(self._ids)
        user, password = self._creds()
        msg = {"v": 2, "id": cid, "cmd": "scan", "ch": self.ch, "timeout": timeout_s, "csv": pri, "csv_fallback": fb,
               "user": user, "password": password}
        self._raw = []
        if not h.send(msg):
            self._terminate_current(force_kill=True)
            return None, {"message": "RGA worker communicate error: stdin closed", "attempt": a, "attempts": n, "cmd": cmd}
        self.sent = (self.sent + [{**msg, "password": "***" if password else ""}])[-200:]
        got = self.clock.wait_until(self.clock.monotonic() + timeout_s,
                                    lambda: cid in self._results or not h.alive() or self._cancel)
        payload = self._results.pop(cid, None)
        if self._cancel:
            self._terminate_current(force_kill=True)
            return None, {"message": "cancelled", "attempt": a, "attempts": n, "cmd": cmd}
        if not got:
            self._terminate_current(force_kill=True)
            return None, {"message": f"RGA worker timeout ({timeout_s}s)", "attempt": a, "attempts": n, "cmd": cmd}
        if payload is None:          # 결과 없이 워커가 끝남(v1: stdout 에 JSON 없음)
            h.wait(2.0)
            rc = h.returncode
            self._terminate_current(force_kill=True)
            return None, {"message": "RGA worker stdout JSON parse failed", "attempt": a, "attempts": n, "cmd": cmd,
                          "returncode": rc, "stdout": _tail("\n".join(self._raw)), "stderr": _tail("\n".join(h.stderr_tail))}
        if not payload.get("ok", False):
            m = payload.get("error") or payload.get("message") or "RGA worker failed"
            return None, {"message": str(m), "attempt": a, "attempts": n, "cmd": cmd, "returncode": None, "meta": payload,
                          "stdout": "", "stderr": _tail("\n".join(h.stderr_tail))}
        return payload, None

    def _scan(self, timeout_s: float, on_data) -> RgaResult:
        n = max(1, int(self._c("RGA_MAX_ATTEMPTS", 3)))
        d = max(0.0, float(self._c("RGA_RETRY_DELAY_S", 1.0)))
        budget = n * float(timeout_s) + max(0, n - 1) * d
        self._emit("status", f"[RGA] 스캔 시작 — 예산 {n}회 x {float(timeout_s):g}s(+대기 {d:g}s) = 총 {budget:g}s")
        self._cancel = False
        self.scanning = True
        try:
            pri, fb = self.csv_paths()
            last_fail: Dict[str, Any] = {"message": "unknown"}
            for a in range(1, n + 1):
                payload, fail = self._attempt(a, n, timeout_s, pri, fb)
                if payload is not None:
                    if payload.get("worker_version") is None:
                        self._emit("status", "[RGA] 구버전 rga_worker.exe — CSV 경로 인자 미지원")
                    elif not payload.get("csv_ok", True):
                        self._emit("status", f"[RGA] CSV 저장 실패(primary={pri or '(기본값)'}): "
                                             f"{payload.get('csv_error') or 'unknown'} → fallback={payload.get('csv_path') or '실패'}")
                    elif payload.get("csv_fallback_used"):
                        self._emit("status", f"[RGA] CSV 정본 저장 실패 → 로컬 폴백에 저장: {payload.get('csv_path')}")
                    mass, pres = payload.get("mass_axis") or [], payload.get("pressures") or []
                    meta = {k: v for k, v in payload.items() if k not in ("v", "id", "kind")}
                    self._emit("data", "RGA data", data={"mass_axis": mass, "pressures": pres, "meta": meta})
                    if on_data:
                        on_data(mass, pres)
                    self._last_ok = self.clock.monotonic()
                    m = f"RGA scan finished (attempt {a}/{n})"
                    self._emit("finished", m)
                    return RgaResult(True, m, mass, pres, meta)
                last_fail = fail or {"message": "unknown", "attempt": a, "attempts": n}
                if self._cancel:
                    return RgaResult(False, "cancelled", fail=last_fail, reason="CANCELLED")
                if a < n:
                    self._emit("status", f"[RGA] attempt {a}/{n} 실패 → 재시도({d}s): {last_fail.get('message')}")
                    if d > 0:
                        self.clock.wait_until(self.clock.monotonic() + d, lambda: self._cancel)
                    continue
            self._emit("failed", str(last_fail.get("message")), "RGA_FAILED", data=last_fail)
            m = f"RGA scan finished (failed after {n})"
            self._emit("finished", m)
            return RgaResult(False, str(last_fail.get("message")), fail=last_fail, reason="RGA_FAILED")
        finally:
            self.scanning = False

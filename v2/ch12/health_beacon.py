"""건강 확인 파일(heartbeat.json) 쓰기 + 멈춤 때 스레드 덤프 — 감시 프로그램(v2/supervisor)의 멈춤 감지 짝(2026-11-13 D408).

감시 프로그램이 환경 변수 CH12_HEARTBEAT_FILE·CH12_HANG_DUMP_FILE 을 주면(감시 아래에서만) 제어 프로그램이 이 비콘을 켠다.
- 1 s 마다 {pid, ready, tick(1초 tick 번호 = 닫은 행 수), cmd(명령 처리 확인 횟수), 설정(enabled·interval·timeout·grace)} 를 파일에 원자적으로 쓴다.
  tick 은 핵심 루프가 실제로 도는지, cmd 는 명령 코어(Ch12Core 잠금)를 0.5 s 안에 잡을 수 있었던 횟수 — '진행 번호가 움직이는지'를 본다.
  HTTP 스레드가 살아 있어도 tick·명령 코어가 멈추면 이 번호가 멈춘다. 비콘 스레드 자신이 멈추면(전체 정지) 파일이 더 이상 바뀌지 않는다.
- 스레드 덤프: faulthandler.dump_traceback_later(한도, repeat) 를 '진행이 있을 때만' 다시 건다. 진행이 한도 동안 없으면 C 감시 스레드가
  (GIL 을 잡은 채 멈췄어도) 모든 스레드의 호출 스택을 CH12_HANG_DUMP_FILE 에 쓴다 — 감시 프로그램은 이 파일 끝을 로그에 옮기고 강제 종료한다.
  준비 전(시작 중)에는 준비 시간(SUPERVISOR_START_GRACE_S) 끝에 맞춰 건다.
- 설정은 매번 cfg 에서 읽는다(LIVE) — 감시 프로그램도 이 파일의 값으로 판단하므로 웹에서 바꾼 값이 바로 적용된다.
"""
from __future__ import annotations

import faulthandler
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

PERIOD_S = 1.0
LOCK_WAIT_S = 0.5


class HealthBeacon:
    def __init__(self, cfg: Dict[str, Any], tick_rows: Callable[[], int], lock: Any, path: Path, dump_path: Optional[Path] = None,
                 ready_fn: Optional[Callable[[], bool]] = None, period_s: float = PERIOD_S,
                 mono: Callable[[], float] = time.monotonic, wall: Callable[[], float] = time.time) -> None:
        self.cfg, self.tick_rows, self.lock = cfg, tick_rows, lock
        self.path, self.dump_path = Path(path), (Path(dump_path) if dump_path else None)
        self.ready_fn = ready_fn
        self.period_s, self.mono, self.wall = period_s, mono, wall
        self.cmd = 0
        self._last_tick: Optional[int] = None
        self._t0 = mono()
        self._dump_f = None
        self._armed = False
        self._stop = threading.Event()
        self._th: Optional[threading.Thread] = None
        self.errors = 0

    # ── 설정(LIVE) ──
    def settings(self) -> Dict[str, Any]:
        c = self.cfg
        return {"enabled": bool(c.get("SUPERVISOR_HANG_ENABLED", True)), "interval": float(c.get("SUPERVISOR_HEALTH_INTERVAL_S", 10.0)),
                "timeout": float(c.get("SUPERVISOR_HANG_TIMEOUT_S", 60.0)), "grace": float(c.get("SUPERVISOR_START_GRACE_S", 300.0))}

    def _probe_cmd(self) -> bool:
        if self.lock is None:
            return True
        try:
            if self.lock.acquire(timeout=LOCK_WAIT_S):
                self.lock.release()
                return True
        except Exception:
            pass
        return False

    # ── 한 번 ──
    def beat(self) -> Dict[str, Any]:
        st = self.settings()
        tick = int(self.tick_rows() or 0)
        ready = tick >= 1 if self.ready_fn is None else bool(self.ready_fn())
        cmd_ok = self._probe_cmd()
        if cmd_ok:
            self.cmd += 1
        moved = self._last_tick is None or tick != self._last_tick
        self._last_tick = tick
        self._arm(st, ready, moved and cmd_ok)
        d = {"pid": os.getpid(), "t_wall": round(self.wall(), 3), "ready": ready, "tick": tick, "cmd": self.cmd, "cmd_ok": cmd_ok, **st}
        self._write(d)
        return d

    def _arm(self, st: Dict[str, Any], ready: bool, progressed: bool) -> None:
        if self.dump_path is None:
            return
        if not st["enabled"]:
            self._cancel()
            return
        if self._dump_f is None:
            try:
                self.dump_path.parent.mkdir(parents=True, exist_ok=True)
                self._dump_f = open(self.dump_path, "a", encoding="utf-8")
            except OSError:
                self.errors += 1
                return
        if not ready:                                           # 시작 중: 준비 시간 끝에 맞춰(남은 시간)
            left = max(1.0, st["grace"] - (self.mono() - self._t0))
            self._set(left, repeat=False)
        elif progressed or not self._armed:                     # 진행이 있을 때만 다시 건다 — 없으면 한도 뒤 덤프
            self._set(max(1.0, st["timeout"]), repeat=True)

    def _set(self, timeout: float, repeat: bool) -> None:
        try:
            faulthandler.cancel_dump_traceback_later()
            faulthandler.dump_traceback_later(timeout, repeat=repeat, file=self._dump_f, exit=False)
            self._armed = True
        except Exception:
            self.errors += 1

    def _cancel(self) -> None:
        if self._armed:
            try:
                faulthandler.cancel_dump_traceback_later()
            except Exception:
                pass
            self._armed = False

    def _write(self, d: Dict[str, Any]) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_name(self.path.name + ".tmp")
            tmp.write_text(json.dumps(d), encoding="utf-8")
            os.replace(tmp, self.path)
        except OSError:
            self.errors += 1

    # ── 스레드 ──
    def start(self) -> None:
        if self._th is not None:
            return

        def run() -> None:
            while not self._stop.is_set():
                try:
                    self.beat()
                except Exception:
                    self.errors += 1
                self._stop.wait(self.period_s)
        self._th = threading.Thread(target=run, name="health-beacon", daemon=True)
        self._th.start()

    def stop(self) -> None:
        self._stop.set()
        if self._th is not None:
            self._th.join(2.0)
        self._cancel()
        if self._dump_f is not None:
            try:
                self._dump_f.close()
            except OSError:
                pass
            self._dump_f = None

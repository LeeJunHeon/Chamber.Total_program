"""TSP 로그 — v1 runtime/tsp_runtime.py 의 _log·로그 파일 준비·_log_writer_loop 파일 출력(PC-129·PC-149).

v1 그대로
- 화면 줄 '[HH:MM:SS] [TSP] {msg}'(pc_logMessage_edit — v2 는 on_ui 콜백), 파일 줄 '[YYYY-mm-dd HH:MM:SS] [TSP] {msg}\\n'.
  메시지가 '[TSP] …' 로 시작해도 '[TSP]' 를 또 붙인다(PC-151 — '[TSP] [TSP] 중단 요청').
- 파일 경로가 아직 없으면(첫 실행 전) 줄은 프리버퍼(최대 1000줄)에 + SYSTEM 일자 로그(출처 'TSP')에.
  경로가 있으면 기록 큐(4096줄, 가득 차면 가장 오래된 줄 1개를 버리고 넣음)로.
- 실행마다 open_run(): {LOG_ROOT_DIR}\\TSP 를 만들고(실패하면 {LOCAL_FALLBACK_ROOT}\\TSP 와 로그
  '[Logger] 로그 폴더 접근 실패(..) → 이번 실행은 로컬에 기록: ..') 경로 TSP_{YYYYmmdd_HHMMSS}.txt(실행 시각) → 기록 시작 →
  프리버퍼를 큐로. 파일은 append·utf-8·newline=""(줄 끝 변환 없음).
- close_run(): 기록기를 멈춘다(파일 닫힘). ⚠ 경로는 지우지 않는다 — 실행 사이의 TSP 로그(예약 로그 등)는 기록기 없는 큐에
  쌓였다가(최대 4096) 다음 실행 파일 앞부분에 기록되고 SYSTEM 로그에도 남지 않는다(PC-149, v1 이상 동작 그대로 — D111).
v2: 파일 쓰기는 outputs.base.QueueWriter(쓰기 스레드)에 줄마다 넣는다(열고 쓰고 닫음 — 내용은 v1 과 같다). v1 기록기는 파일을
  못 열면 줄을 큐에 되돌려 0.1 s 마다 다시 시도했다 — v2 는 그 줄을 쓰기 오류(writer.errors)로 남긴다.
"""
from __future__ import annotations

import collections
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Deque, Mapping, Optional

from v2.ch12.outputs.base import Owned, QueueWriter, wall_now
from v2.ch12.outputs.paths import Paths
from v2.ch12.outputs.system_log import SystemLog

PRESTART_MAX = 1000        # runtime/tsp_runtime.py:95
QUEUE_MAX = 4096           # runtime/tsp_runtime.py:97


def append_line(path: Path, line: str) -> None:
    with open(path, "a", encoding="utf-8", newline="") as f:
        f.write(line)


class TspTxtLog(Owned):
    def __init__(self, cfg: Optional[Mapping[str, Any]] = None, *, writer: Optional[QueueWriter] = None,
                 now: Callable[[], datetime] = wall_now, on_ui: Optional[Callable[[str], None]] = None,
                 system_log: Optional[SystemLog] = None) -> None:
        super().__init__(writer, "tsp-log")
        self.cfg = cfg
        self.now = now
        self.on_ui = on_ui
        self.system_log = system_log if system_log is not None else SystemLog(cfg, writer=self.writer, now=now)
        self.path: Optional[Path] = None
        self.active = False                                   # v1 _log_writer_task 가 도는 중
        self.prestart: Deque[str] = collections.deque(maxlen=PRESTART_MAX)
        self.pending: Deque[str] = collections.deque(maxlen=QUEUE_MAX)   # 기록기 없는 동안의 큐(PC-149)
        self._lock = threading.Lock()

    # ───────── v1 _log ─────────
    def log(self, msg: str) -> str:
        """한 줄(화면·파일). 화면 줄을 돌려준다."""
        t = self.now()
        ui = f"[{t.strftime('%H:%M:%S')}] [TSP] {msg}"
        line = f"[{t.strftime('%Y-%m-%d %H:%M:%S')}] [TSP] {msg}\n"
        if self.on_ui is not None:
            try:
                self.on_ui(ui)
            except Exception:
                pass
        with self._lock:
            if self.path is None:
                self.prestart.append(line)
                to_system = True
            else:
                self._enqueue(line)
                to_system = False
        if to_system:
            self.system_log.append("TSP", msg)
        return ui

    def _enqueue(self, line: str) -> None:
        if self.active and self.path is not None:
            self.writer.put(append_line, self.path, line)
        else:
            self.pending.append(line)                         # deque maxlen — 가득 차면 가장 오래된 줄을 버림(v1 QueueFull)

    # ───────── 실행마다 ─────────
    def log_dir(self) -> Path:
        return Paths(self.cfg).log_root() / "TSP"

    def open_run(self, log: Callable[[str], Any]) -> Path:
        """v1 _run 의 '로그 파일 준비'(tsp_runtime.py:555-574). log = 엔진의 _log(폴백 안내 줄을 같은 길로)."""
        ts = self.now().astimezone().strftime("%Y%m%d_%H%M%S")
        d = self.log_dir()
        try:
            d.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            d = Paths(self.cfg).fallback_root() / "TSP"
            try:
                d.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
            log(f"[Logger] 로그 폴더 접근 실패({e!r}) → 이번 실행은 로컬에 기록: {d}")
        path = (d / f"TSP_{ts}").with_suffix(".txt")
        with self._lock:
            self.path = path
            self.active = True
            for line in list(self.pending):                   # 실행 사이에 쌓인 줄이 먼저(PC-149)
                self.writer.put(append_line, path, line)
            self.pending.clear()
            for line in list(self.prestart):
                self.writer.put(append_line, path, line)
            self.prestart.clear()
        return path

    def close_run(self) -> None:
        """기록기 취소(파일 닫힘) — 경로는 남긴다(PC-149)."""
        with self._lock:
            self.active = False

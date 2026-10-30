"""공정 txt 로그 — v1 runtime/chamber_runtime.py 의 런 로그(append_log·_open_run_log·_log_writer_loop·_close_run_log) 파일 출력.

v1 과 같은 것(LOG-007·009~017)
- 경로 {LOG_ROOT_DIR}\\CH{n}\\CH{n}_{안전 공정명}_{YYYYmmdd_HHMMSS_mmm}.txt(충돌 검사 없음). 공정명: process_note →
  Process_name 순(소문자 process_name 은 보지 않음), 비면 Untitled; \\/:*?"<>| 묶음→'_', 연속 공백→' ', 공백→'_', 앞뒤 ' .'
  제거, 60자, 비면 Untitled(chamber_runtime.py:7310-7341).
- 머리 줄 '# ==== Sputter Run ====' … '# ============================'(7348-7365), 그 뒤 프리스타트 버퍼(최대 1000줄), 그 뒤
  '[..] [CHn:Logger] 새 로그 파일 시작: <이름>'.
- 본문 줄 '[YYYY-mm-dd HH:MM:SS] [CH{n}:{출처}] {메시지}\\n'. 파일이 열리기 전 줄은 프리스타트 버퍼 + SYSTEM 일자 로그(일부 중복은
  v1 의도). 화면 줄은 '[HH:MM:SS] [CH{n}:{출처}] {메시지}'(on_ui 콜백).
- 기록 큐 4096줄, 가득 차면 가장 오래된 줄 1개 버림(7411-7417). utf-8(BOM 없음), newline=""(줄 끝 변환 없음 — '\\n').
- 쓰기 실패 → LOCAL_FALLBACK_CH{n}_DIR\\<같은 이름> 으로 1회 전환('세션 로그를 로컬 폴백으로 전환: <경로>' 화면 1회).
  폴백도 실패하면 CH{n}_{YYYYmmdd_HHMMSS}_writer_pending.txt 긴급 보관(7487-7514).
- 열리는 시점: 엔진이 공정 시작 처리 중 '값 검사 전'에 open_run 을 부른다(LOG-017 — 입력값 오류로 거부돼도 파일이 생김.
  v1 그대로). 큐 공정은 행마다 close_run → open_run(새 파일).
- 끝: 엔진은 END 전에 FOOTER_SETTLE_S(0.3 s) 기다린 뒤 close_run(늦은 장치 줄을 받기 위해 — 7383-7398). 경로가 없으면 END
  를 넣지 않는다(END 만 있는 파일 방지).
- 폴백 폴더는 시작 때 정한 값(v1 은 설정을 다시 읽어도 바뀌지 않았다 — LOG-020).

v2 에서 다른 것: 줄마다 (경로, 줄)을 같이 넣는다 — v1 은 쓰기 순간의 경로로 썼다(경로가 바뀐 직후 묶음이 새 파일로 갈 수
있었다). 종료 drain 파일(shutdown_drain)은 v2 에서 생기지 않는다(경로 없는 줄은 큐에 넣지 않으므로).

부르는 법(엔진·이벤트에서)
  t = ProcessTxtLog(ch, cfg, system_log=SystemLog(cfg, writer=w), writer=w, now=clock_dt, on_ui=fn)
  t.log("Process", "메시지")            # 어디서든(파일이 없으면 프리버퍼 + SYSTEM)
  t.side_log("THK", "공정 무관 줄")       # 화면 + SYSTEM 만
  path = t.open_run(params, origin="host", origin_meta={"request_id":.., "peer":..})
  t.close_run()
  heater_status_line(...) — CH2 히터 상태 한 줄(HEATER_RUN_LOG_PERIOD_S 마다 엔진이 t.log("HEATER", ..))
"""
from __future__ import annotations

import collections
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Deque, Mapping, Optional

from v2.ch12.outputs.base import Owned, QueueWriter, wall_now
from v2.ch12.outputs.log_hub import SessionTextAppender
from v2.ch12.outputs.paths import Paths
from v2.ch12.outputs.system_log import SystemLog

PRESTART_MAX = 1000          # chamber_runtime.py:562
QUEUE_MAX = 4096             # chamber_runtime.py:570
FOOTER_SETTLE_S = 0.3        # chamber_runtime.py:213 (_RUN_LOG_FOOTER_SETTLE_S)
_CLOSE = object()


def safe_process_name(params: Mapping[str, Any]) -> str:
    raw_name = str(params.get("process_note") or params.get("Process_name") or "").strip()
    if not raw_name:
        raw_name = "Untitled"
    safe_name = re.sub(r'[\\/:*?"<>|]+', "_", raw_name)
    safe_name = re.sub(r"\s+", " ", safe_name).strip()
    safe_name = safe_name.replace(" ", "_")
    safe_name = safe_name.strip(" .")
    return safe_name[:60] if safe_name else "Untitled"


def heater_status_line(*, pv: Optional[float], tc_ok: bool, sv_ramp: Optional[float], target: Optional[float],
                       mv: Optional[float], mv_limit: Optional[float], holding: bool, hold_value: Optional[float],
                       ramp_active: bool, owner_label: str) -> str:
    """공정 txt 히터 상태 한 줄(v1 _heater_status_line, chamber_runtime.py:7041-7064)."""
    def f1(v) -> str:
        return "--.-" if v is None else f"{float(v):.1f}"

    def i0(v) -> str:
        return "-" if v is None else f"{int(v)}"
    pv_ = pv if tc_ok else None
    if holding:
        mode = f"유지(DAC 상한 {i0(hold_value)})"
    elif ramp_active:
        mode = "램프"
    else:
        mode = "제어"
    return (f"히터 상태 — PV {f1(pv_)}°C · SV {f1(sv_ramp)}°C · 목표 {f1(target)}°C · "
            f"MV {i0(mv)}/{i0(mv_limit)} · {mode} · {owner_label or '-'}")


class ProcessTxtLog(Owned):
    def __init__(self, ch: int, cfg: Optional[Mapping[str, Any]] = None, *, system_log: Optional[SystemLog] = None,
                 writer: Optional[QueueWriter] = None, now: Callable[[], datetime] = wall_now,
                 on_ui: Optional[Callable[[str], None]] = None) -> None:
        super().__init__(writer, f"process-txt-ch{int(ch)}")
        self.ch = int(ch)
        self.paths = Paths(cfg)
        self.now = now
        self.on_ui = on_ui
        self.system_log = system_log if system_log is not None else SystemLog(cfg, writer=self.writer, now=now)
        self._local_dir = self.paths.fallback_ch(self.ch)          # 시작 때 고정(LOG-020)
        self._log_dir = self.paths.process_txt_dir(self.ch)
        self._appender = SessionTextAppender(fallback_dir=self._local_dir, encoding="utf-8")
        self._prestart: Deque[str] = collections.deque(maxlen=PRESTART_MAX)
        self._q: Deque[tuple] = collections.deque(maxlen=QUEUE_MAX)    # 가득 차면 가장 오래된 것부터 버림(v1)
        self._qlk = threading.Lock()
        self._drain_scheduled = False
        self._path: Optional[Path] = None
        self.dropped = 0
        self.writer.put(self._ensure_log_dir)

    # ── 상태 ──
    @property
    def path(self) -> Optional[Path]:
        return self._path

    @property
    def current_file(self) -> Optional[Path]:
        """실제로 쓰고 있는 파일(폴백이면 폴백 경로)."""
        return self._appender.current_path

    def _ui(self, line: str) -> None:
        if self.on_ui is not None:
            try:
                self.on_ui(line)
            except Exception:
                pass

    # ── 줄 넣기 ──
    def log(self, source: str, msg: str) -> None:
        now = self.now()
        line_ui = f"[{now.strftime('%H:%M:%S')}] [CH{self.ch}:{source}] {msg}"
        line_file = f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [CH{self.ch}:{source}] {msg}\n"
        self._ui(line_ui)
        if self._path is None:
            self._prestart.append(line_file)
            self.system_log.append(f"CH{self.ch}:{source}", msg)
            return
        self._enqueue(self._path, line_file)

    def side_log(self, source: str, msg: str) -> None:
        """화면과 SYSTEM 일자 로그에만(공정 txt·프리버퍼에는 넣지 않음 — append_side_log)."""
        now = self.now()
        self._ui(f"[{now.strftime('%H:%M:%S')}] [CH{self.ch}:{source}] {msg}")
        self.system_log.append(f"CH{self.ch}:{source}", msg)

    def _enqueue(self, path: Any, line: Any) -> None:
        with self._qlk:
            if len(self._q) == self._q.maxlen:
                self.dropped += 1
            self._q.append((path, line))
            if self._drain_scheduled:
                return
            self._drain_scheduled = True
        self.writer.put(self._drain)

    # ── 열기·닫기 ──
    def open_run(self, params: Mapping[str, Any], *, origin: str = "ui",
                 origin_meta: Optional[Mapping[str, Any]] = None) -> Path:
        now_local = self.now()
        safe_name = safe_process_name(params)
        ts_ms = now_local.strftime("%Y%m%d_%H%M%S_%f")[:-3]
        base = (self._log_dir / f"CH{self.ch}_{safe_name}_{ts_ms}").with_suffix(".txt")
        self._path = base
        name = (params.get("process_note") or params.get("Process_name") or f"Run CH{self.ch}")
        put = lambda s: self._enqueue(base, s)   # noqa: E731
        put("# ==== Sputter Run ====\n")
        put(f"# started_at = {self.now().isoformat()}\n")
        put(f"# chamber = CH{self.ch}\n")
        put(f"# process_name = {name}\n")
        org = str(origin or "ui")
        put(f"# origin = {org}\n")
        if org == "host":
            m = dict(origin_meta or {})
            put(f"# request_id = {m.get('request_id', '')} peer = {m.get('peer', '')}\n")
        if "process_time" in params:
            put(f"# time_min = {float(params.get('process_time', 0) or 0):.2f}\n")
        put("# ============================\n")
        for line in list(self._prestart):
            put(line)
        self._prestart.clear()
        self.log("Logger", f"새 로그 파일 시작: {base.name}")
        return base

    def close_run(self) -> None:
        """END 를 넣고 파일을 닫는다(경로가 없으면 아무것도 안 함). 엔진이 FOOTER_SETTLE_S 를 먼저 기다린다."""
        p = self._path
        if p is None:
            return
        self._enqueue(p, "# ==== END ====\n")
        self._enqueue(p, _CLOSE)
        self._path = None

    # ── 쓰기 스레드 ──
    def _ensure_log_dir(self) -> None:
        try:
            self._log_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            self._ui(f"[Logger] NAS 폴더 접근 실패 → 실제 기록 시 CH{self.ch} 로컬 폴백 예정: {self._local_dir}")

    def _drain(self) -> None:
        with self._qlk:
            items = list(self._q)
            self._q.clear()
            self._drain_scheduled = False
        i = 0
        while i < len(items):
            path, line = items[i]
            if line is _CLOSE:
                self._appender.close()
                i += 1
                continue
            j = i
            parts = []
            while j < len(items) and items[j][0] == path and items[j][1] is not _CLOSE:
                parts.append(items[j][1])
                j += 1
            self._write(path, "".join(parts))
            i = j

    def _write(self, path: Path, text: str) -> None:
        try:
            self._appender.set_primary_path(path)
            self._appender.write(text)
            if self._appender.consume_switched_flag():
                self._ui(f"[{self.now().strftime('%H:%M:%S')}] [CH{self.ch}:Logger] 세션 로그를 로컬 폴백으로 전환: "
                         f"{self._appender.current_path}")
        except Exception as e:
            try:
                self._local_dir.mkdir(parents=True, exist_ok=True)
                emergency = self._local_dir / f"CH{self.ch}_{self.now():%Y%m%d_%H%M%S}_writer_pending.txt"
                with open(emergency, "a", encoding="utf-8", newline="") as fp:
                    fp.write("# [log-writer pending]\n")
                    fp.write(f"# source_path = {path}\n")
                    fp.write(f"# reason = {e!r}\n")
                    fp.write(text)
                self._ui(f"[{self.now().strftime('%H:%M:%S')}] [CH{self.ch}:Logger] 세션 로그 긴급 보관 파일 생성: {emergency}")
            except Exception:
                pass

    def stop(self, timeout_s: float = 5.0) -> None:
        self.close_run()
        self.writer.put(self._appender.close)
        self.stop_writer(timeout_s)

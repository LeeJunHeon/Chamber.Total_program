"""런타임 덤프 — v1 util/runtime_dump.py 의 파일 출력을 v2 상태 사전으로(LOG-089·090).

v1 그대로
- {LOG_ROOT_DIR}\\RUNTIME_DUMP\\runtime_dump_{YYYYmmdd_HHMMSS}_{사유}_pid{pid}.txt(폴더 실패 시 LOCAL_FALLBACK_ROOT\\RUNTIME_DUMP)
- 내용 '=== SNAPSHOT(JSON) ===\\n' + json(ensure_ascii=False, indent=2) + '\\n\\n=== THREAD TRACEBACK (faulthandler) ===\\n'
  + faulthandler 모든 스레드 + '\\n[end]\\n'. utf-8 텍스트 모드(v1 과 같은 open).
- 스냅샷 키 meta(timestamp·pid·python·executable·argv·platform·cwd)·threads(name·ident·daemon·alive)·ui·asyncio·objects.
- 동시에 1개만(진행 중이면 RuntimeError('runtime dump already in progress')). 쓰기는 별도 스레드(v1 runtime-dump-writer).
- 사유: 화면 RuntimeDump 버튼 'manual_button', 공정 중 종료 시도가 막혔을 때 'close_blocked_while_running'(main.py:1502-1505).
v2: ui 는 v2 에 Qt 위젯이 없어 화면(HMI)이 준 값 사전(없으면 v1 빈 화면과 같은 빈 분류 사전), asyncio 는
{"available": false, "reason": "loop is None"}(v2 에 asyncio 없음), objects 는 v2 상태 사전(키 → 값 사전은 v1
_summarize_object 의 dict 규칙대로 값마다 repr(400자)).

부르는 법
  path = request_dump(cfg, state={"runtime_state":..., "plc": {...}, "ch1": {...}, ...}, reason="manual_button", ui=None)
"""
from __future__ import annotations

import faulthandler
import json
import os
import platform
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from v2.ch12.outputs.paths import Paths

_DUMP_LOCK = threading.Lock()
_DUMP_IN_PROGRESS = False
UI_EMPTY = {"plain_text": {}, "text": {}, "line_edit": {}, "spin": {}, "combo": {}, "buttons": {}, "labels": {},
            "misc": {}}


def _safe_repr(v: Any, max_len: int = 400) -> str:
    try:
        s = repr(v)
    except Exception:
        s = f"<unreprable {type(v).__name__}>"
    return s if len(s) <= max_len else s[:max_len] + " ...[truncated]"


def summarize(obj: Any) -> Any:
    """v1 _summarize_object 의 dict·None 규칙(v2 상태는 사전이다). 사전이 아니면 repr."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {str(k): _safe_repr(v) for k, v in obj.items()}
    return {"type": f"{type(obj).__module__}.{type(obj).__name__}", "repr": _safe_repr(obj)}


def collect_snapshot(state: Optional[Mapping[str, Any]], ui: Optional[Mapping[str, Any]] = None,
                     strftime: Callable[[str], str] = time.strftime) -> dict:
    snap: dict = {}
    snap["meta"] = {
        "timestamp": strftime("%Y-%m-%d %H:%M:%S"),
        "pid": os.getpid(),
        "python": sys.version,
        "executable": sys.executable,
        "argv": sys.argv,
        "platform": platform.platform(),
        "cwd": os.getcwd(),
    }
    snap["threads"] = [{"name": t.name, "ident": t.ident, "daemon": t.daemon, "alive": t.is_alive()}
                       for t in threading.enumerate()]
    snap["ui"] = dict(ui) if ui is not None else {k: {} for k in UI_EMPTY}
    snap["asyncio"] = {"available": False, "reason": "loop is None"}
    snap["objects"] = {}
    for k, obj in (state or {}).items():
        snap["objects"][k] = summarize(obj)
    return snap


def write_dump(out_path: Path, snapshot: dict) -> None:
    with out_path.open("w", encoding="utf-8") as f:
        f.write("=== SNAPSHOT(JSON) ===\n")
        f.write(json.dumps(snapshot, ensure_ascii=False, indent=2))
        f.write("\n\n=== THREAD TRACEBACK (faulthandler) ===\n")
        try:
            f.flush()
            faulthandler.dump_traceback(file=f, all_threads=True)
        except Exception as e:
            f.write(f"(faulthandler dump failed: {e!r})\n")
        f.write("\n[end]\n")


def dump_dir(cfg: Optional[Mapping[str, Any]] = None) -> Path:
    primary, fb = Paths(cfg).dump_dirs()
    try:
        primary.mkdir(parents=True, exist_ok=True)
        return primary
    except Exception:
        fb.mkdir(parents=True, exist_ok=True)
        return fb


def request_dump(cfg: Optional[Mapping[str, Any]] = None, *, state: Optional[Mapping[str, Any]] = None,
                 reason: str = "manual", ui: Optional[Mapping[str, Any]] = None, wait: bool = False,
                 strftime: Callable[[str], str] = time.strftime) -> Path:
    global _DUMP_IN_PROGRESS
    with _DUMP_LOCK:
        if _DUMP_IN_PROGRESS:
            raise RuntimeError("runtime dump already in progress")
        _DUMP_IN_PROGRESS = True
    try:
        root = dump_dir(cfg)
        out_path = root / f"runtime_dump_{strftime('%Y%m%d_%H%M%S')}_{reason}_pid{os.getpid()}.txt"
        snapshot = collect_snapshot(state, ui, strftime)
    except Exception:
        with _DUMP_LOCK:
            _DUMP_IN_PROGRESS = False
        raise

    def _writer() -> None:
        global _DUMP_IN_PROGRESS
        try:
            write_dump(out_path, snapshot)
        finally:
            with _DUMP_LOCK:
                _DUMP_IN_PROGRESS = False

    t = threading.Thread(target=_writer, name="runtime-dump-writer", daemon=True)
    t.start()
    if wait:
        t.join(10.0)
    return out_path

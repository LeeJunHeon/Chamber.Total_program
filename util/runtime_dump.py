# util/runtime_dump.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import faulthandler
import json
import os
import platform
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Optional

_DEFAULT_LOG_ROOT = Path(r"\\VanaM_NAS\VanaM_Sputter\Sputter\Logs\CH1&2")

_DUMP_LOCK = threading.Lock()
_DUMP_IN_PROGRESS = False

_SKIP_ATTRS = {
    "ui", "_loop", "loop", "app", "qapp", "chat",
    "plc", "_plc", "mfc", "mfc_gas", "mfc_pressure", "ig",
}

def _safe_mkdir(p: Path) -> Path:
    try:
        p.mkdir(parents=True, exist_ok=True)
        return p
    except Exception:
        # ✅ ERROR 폴더 아래가 아니라 CH1&2 아래에 RUNTIME_DUMP 생성
        fallback = Path.cwd() / "Logs" / "CH1&2" / "RUNTIME_DUMP"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback

def _dump_root(log_root: Optional[Path]) -> Path:
    base = Path(log_root) if log_root is not None else _DEFAULT_LOG_ROOT
    # ✅ CH1&2 바로 아래에 RUNTIME_DUMP 생성
    return _safe_mkdir(base / "RUNTIME_DUMP")

def _safe_repr(v: Any, max_len: int = 400) -> str:
    try:
        s = repr(v)
    except Exception:
        s = f"<unreprable {type(v).__name__}>"
    return s if len(s) <= max_len else s[:max_len] + " ...[truncated]"

def _tail_lines(text: str, max_lines: int = 300) -> str:
    if not text:
        return ""
    lines = text.splitlines()
    return text if len(lines) <= max_lines else "\n".join(lines[-max_lines:])

def _collect_ui_values(ui: Any, max_log_lines: int) -> dict:
    from PySide6.QtWidgets import (
        QPlainTextEdit, QTextEdit, QLineEdit,
        QAbstractButton, QComboBox, QSpinBox, QDoubleSpinBox, QLabel
    )

    out = {
        "plain_text": {}, "text": {}, "line_edit": {}, "spin": {},
        "combo": {}, "buttons": {}, "labels": {}, "misc": {}
    }

    for name, w in vars(ui).items():
        try:
            if isinstance(w, QPlainTextEdit):
                txt = w.toPlainText()
                if name.endswith("logMessage_edit") or "logMessage" in name:
                    txt = _tail_lines(txt, max_lines=max_log_lines)
                out["plain_text"][name] = txt
            elif isinstance(w, QTextEdit):
                out["text"][name] = w.toPlainText()
            elif isinstance(w, QLineEdit):
                out["line_edit"][name] = w.text()
            elif isinstance(w, (QSpinBox, QDoubleSpinBox)):
                out["spin"][name] = {"value": w.value(), "text": w.text()}
            elif isinstance(w, QComboBox):
                out["combo"][name] = {"index": w.currentIndex(), "text": w.currentText()}
            elif isinstance(w, QAbstractButton):
                out["buttons"][name] = {
                    "text": w.text(),
                    "checked": bool(getattr(w, "isChecked", lambda: False)()),
                    "enabled": w.isEnabled(),
                    "visible": w.isVisible(),
                }
            elif isinstance(w, QLabel):
                out["labels"][name] = w.text()
        except Exception as e:
            out["misc"][name] = f"(failed: {e!r})"

    return out

def _collect_asyncio(loop: asyncio.AbstractEventLoop) -> dict:
    info = {"available": True, "tasks": []}
    try:
        tasks = asyncio.all_tasks(loop)
    except Exception as e:
        return {"available": False, "reason": f"all_tasks failed: {e!r}"}

    for t in list(tasks):
        tinfo = {
            "repr": _safe_repr(t),
            "name": getattr(t, "get_name", lambda: "")(),
            "done": t.done(),
            "cancelled": t.cancelled(),
        }
        try:
            tinfo["coro"] = _safe_repr(t.get_coro())
        except Exception:
            pass

        try:
            frames = t.get_stack(limit=50)
            stacks = []
            for fr in frames:
                stacks.append("".join(traceback.format_stack(fr, limit=30)))
            tinfo["stack"] = stacks
        except Exception as e:
            tinfo["stack_error"] = _safe_repr(e)

        info["tasks"].append(tinfo)

    return info

def _summarize_object(obj: Any) -> Any:
    if obj is None:
        return None

    # dict는 그대로(값은 repr)
    if isinstance(obj, dict):
        return {str(k): _safe_repr(v) for k, v in obj.items()}

    out: dict[str, Any] = {"type": f"{type(obj).__module__}.{type(obj).__name__}"}

    # is_connected 같은 동기 상태는 있으면 저장
    for attr in ("is_connected", "connected", "_running", "running"):
        try:
            if hasattr(obj, attr):
                v = getattr(obj, attr)
                out[attr] = _safe_repr(v() if callable(v) else v)
        except Exception as e:
            out[attr] = f"(failed: {e!r})"

    try:
        d = vars(obj)
    except Exception:
        out["repr"] = _safe_repr(obj)
        return out

    state: dict[str, Any] = {}
    for k, v in d.items():
        if k in _SKIP_ATTRS:
            continue
        state[k] = _safe_repr(v)
    out["state"] = state
    return out

def collect_snapshot(ui: Any, loop: Optional[asyncio.AbstractEventLoop], extra_objects: Optional[dict[str, Any]]) -> dict:
    snap: dict[str, Any] = {}
    snap["meta"] = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "pid": os.getpid(),
        "python": sys.version,
        "executable": sys.executable,
        "argv": sys.argv,
        "platform": platform.platform(),
        "cwd": os.getcwd(),
    }
    snap["threads"] = [
        {"name": t.name, "ident": t.ident, "daemon": t.daemon, "alive": t.is_alive()}
        for t in threading.enumerate()
    ]
    snap["ui"] = _collect_ui_values(ui, max_log_lines=300)
    snap["asyncio"] = _collect_asyncio(loop) if loop is not None else {"available": False, "reason": "loop is None"}

    snap["objects"] = {}
    if extra_objects:
        for k, obj in extra_objects.items():
            snap["objects"][k] = _summarize_object(obj)

    return snap

def request_dump(
    *,
    ui: Any,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    log_root: Optional[Path] = None,
    extra_objects: Optional[dict[str, Any]] = None,
    reason: str = "manual",
) -> Path:
    global _DUMP_IN_PROGRESS

    with _DUMP_LOCK:
        if _DUMP_IN_PROGRESS:
            raise RuntimeError("runtime dump already in progress")
        _DUMP_IN_PROGRESS = True

    root = _dump_root(log_root)
    ts = time.strftime("%Y%m%d_%H%M%S")
    pid = os.getpid()
    out_path = root / f"runtime_dump_{ts}_{reason}_pid{pid}.txt"

    # ✅ UI 스레드에서 먼저 snapshot 수집 (Qt 위젯 접근 안전)
    snapshot = collect_snapshot(ui, loop, extra_objects)

    def _writer():
        global _DUMP_IN_PROGRESS
        try:
            with out_path.open("w", encoding="utf-8") as f:
                f.write("=== SNAPSHOT(JSON) ===\n")
                f.write(json.dumps(snapshot, ensure_ascii=False, indent=2))
                f.write("\n\n=== THREAD TRACEBACK (faulthandler) ===\n")
                try:
                    faulthandler.dump_traceback(file=f, all_threads=True)
                except Exception as e:
                    f.write(f"(faulthandler dump failed: {e!r})\n")
                f.write("\n[end]\n")
        finally:
            with _DUMP_LOCK:
                _DUMP_IN_PROGRESS = False

    threading.Thread(target=_writer, name="runtime-dump-writer", daemon=True).start()
    return out_path
"""런타임 덤프 부르기 — v1 main.py _build_runtime_dump_extra(1170-1185)·_request_runtime_dump_internal(1187-1207)·
_on_runtime_dump_clicked(1209-)·종료 차단 때 1회 덤프(1500-1505)를 v2 상태 사전으로(outputs/runtime_dump.request_dump).

dump_runtime(system, reason, ui=None) → 저장 경로(str) 또는 None
- 사유: 화면 RuntimeDump 버튼 'manual_button', 공정 중 종료 시도가 막혔을 때 'close_blocked_while_running'(dump_close_blocked —
  프로그램 실행 한 번에 1회만, v1 _close_block_dumped_once).
- 상태(objects): runtime_state(run_state.snapshot — 조립이 system.run_state 로 둔 것), closing_started·close_ready(있으면),
  엔진마다(proc.ch1·proc.ch2·pc·tsp·heater …) state_dict()/state()/status() 중 있는 것, 장비 표(device_table), 1초 tick 통계,
  기록 대기열 크기, 묶은 행 수, 최근 내부 이벤트 20건. v1 은 plc·mfc·ig 객체의 속성을 repr 로 남겼다 — v2 는 장비 상태가
  1초 행·기록에 있으므로 장비 표(블록 on/off)만 넣는다.
- 결과 로그(v1 _broadcast_log 그대로의 태그·문구 — v2 는 내부 이벤트 source='dump', kind='log'):
  'WARN/DUMP'  'Runtime dump saved({reason}): {path}'
  'WARN/DUMP'  'Runtime dump skipped({reason}): {e}'      (이미 진행 중 — RuntimeError)
  'ERROR/DUMP' 'Runtime dump failed({reason}): {e!r}'
명령 연결(화면 버튼·종료 차단)은 4단계 core 가 부른다 — 여기는 함수만.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Mapping, Optional

from v2.ch12.outputs import runtime_dump as RD

_STATE_FNS = ("state_dict", "state", "status")


def _engine_state(obj: Any) -> Any:
    for name in _STATE_FNS:
        f = getattr(obj, name, None)
        if callable(f):
            try:
                return f()
            except Exception as e:
                return {"error": f"{name}() 실패: {e!r}"}
    return obj


def collect_state(system) -> Dict[str, Any]:
    """v2 상태 사전(덤프 objects) — 실패한 항목은 그 자리에 오류 문구."""
    out: Dict[str, Any] = {}

    def put(key: str, fn: Callable[[], Any]) -> None:
        try:
            out[key] = fn()
        except Exception as e:
            out[key] = {"error": repr(e)}

    rs = getattr(system, "run_state", None)
    put("runtime_state", lambda: rs.snapshot() if rs is not None else None)
    for k in ("closing_started", "close_ready"):
        if hasattr(system, k):
            out[k] = getattr(system, k)
    put("meta", lambda: {"mode": getattr(system, "mode", None), "program": getattr(system, "program", None),
                         "version": getattr(system, "version", None), "started": getattr(system, "started", None),
                         "internal_errors": getattr(system, "internal_errors", None),
                         "engines": list(getattr(system, "engines", {}) or {})})
    for name, eng in list((getattr(system, "engines", {}) or {}).items()):
        put(f"engine:{name}", lambda e=eng: _engine_state(e))
    if hasattr(system, "device_table"):
        put("devices", system.device_table)
    if hasattr(system, "tick_stats"):
        put("tick_stats", system.tick_stats)
    rec = getattr(system, "recorder", None)
    put("recorder", lambda: {"pending": rec._q.qsize(), "dropped": rec.dropped} if rec is not None else None)
    outs = getattr(system, "outputs", None)
    put("outputs", lambda: {"pending": getattr(outs, "pending", None)} if outs is not None else None)
    evs = list(getattr(system, "events", []) or [])[-20:]
    put("events_recent", lambda: {str(i): f"{e.source} {e.kind} {e.code or ''} {e.detail}"[:300] for i, e in enumerate(evs)})
    loop = getattr(system, "loop", None)
    if loop is not None:
        put("tick_loop", lambda: {"rows": getattr(loop, "rows", None)})
    return out


def _say(system, tag: str, text: str, level: str) -> None:
    try:
        system.emit("dump", "log", text, level=level, reason=tag)
    except Exception:
        pass


def dump_runtime(system, reason: str, ui: Optional[Mapping[str, Any]] = None, *, wait: bool = False) -> Optional[str]:
    """런타임 덤프 1회(v1 _request_runtime_dump_internal). 저장 경로 또는 None(진행 중·실패 — 로그로 알림)."""
    try:
        path = RD.request_dump(getattr(system, "cfg", None), state=collect_state(system), reason=str(reason), ui=ui,
                               wait=wait)
    except RuntimeError as e:
        _say(system, "WARN/DUMP", f"Runtime dump skipped({reason}): {e}", "warn")
        return None
    except Exception as e:
        _say(system, "ERROR/DUMP", f"Runtime dump failed({reason}): {e!r}", "error")
        return None
    _say(system, "WARN/DUMP", f"Runtime dump saved({reason}): {path}", "warn")
    return str(path)


def dump_close_blocked(system, ui: Optional[Mapping[str, Any]] = None, *, wait: bool = False) -> Optional[str]:
    """공정 중 종료 시도가 막혔을 때 — 실행 한 번에 1회만(v1 _close_block_dumped_once)."""
    if getattr(system, "_close_block_dumped_once", False):
        return None
    system._close_block_dumped_once = True
    return dump_runtime(system, "close_blocked_while_running", ui, wait=wait)

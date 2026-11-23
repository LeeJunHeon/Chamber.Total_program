"""윈도우 프로세스 우선순위 — sim 은 낮게(2026-11-23 D429).

sim 은 v1(운영)이 도는 장비 노트북에서 함께 뜬다 — CPU 를 v1 보다 먼저 가져가지 않게 BelowNormal 로 낮춘다.
자식(OES·RGA 워커 등 — 우선순위 플래그 없이 띄움)도 같은 우선순위를 물려받는다(CreateProcess: 부모가 IDLE·BELOW_NORMAL 이면
자식의 기본 우선순위 = 부모 것). 그래서 실행 cmd 의 'start /belownormal /b'(콘솔 Ctrl+C 를 끔) 대신 프로그램이 스스로 낮춘다.
"""
from __future__ import annotations

import sys
from typing import Any, Optional

BELOW_NORMAL_PRIORITY_CLASS = 0x00004000
LOW_LINE = "우선순위 낮음(BelowNormal — 자식 워커도 물려받음, v1 운영보다 뒤)"


def lower_priority(kernel32: Any = None) -> Optional[str]:
    """윈도우면 이 프로세스를 BelowNormal 로 낮추고 결과 한 줄, 윈도우가 아니면 None(바꾸지 않음). kernel32 는 시험용."""
    if kernel32 is None and sys.platform != "win32":
        return None
    try:
        import ctypes
        from ctypes import wintypes
        k = kernel32 if kernel32 is not None else ctypes.WinDLL("kernel32", use_last_error=True)
        k.GetCurrentProcess.restype = wintypes.HANDLE
        k.SetPriorityClass.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        k.SetPriorityClass.restype = wintypes.BOOL
        k.GetPriorityClass.argtypes = [wintypes.HANDLE]
        k.GetPriorityClass.restype = wintypes.DWORD
        h = k.GetCurrentProcess()
        if not k.SetPriorityClass(h, BELOW_NORMAL_PRIORITY_CLASS):
            return f"우선순위를 낮추지 못함(오류 {ctypes.get_last_error()}) — 보통 우선순위로 계속"
        got = int(k.GetPriorityClass(h))
        if got != BELOW_NORMAL_PRIORITY_CLASS:
            return f"우선순위 확인 값 0x{got:X} — BelowNormal(0x4000)이 아님"
        return LOW_LINE
    except Exception as e:
        return f"우선순위를 낮추지 못함: {e!r} — 보통 우선순위로 계속"

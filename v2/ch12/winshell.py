"""윈도우 전용 — 콘솔 창 닫기·로그오프·윈도우 종료를 받아 의도적 종료로(사용자 지시 2장 8) 윈도우 전용 부분 분리).

챔버K v2/chk/app.py(install_handlers 의 SetConsoleCtrlHandler, start_session_window·_session_loop 의 숨은 창)에서 옮겼다.
- SetConsoleCtrlHandler: CTRL_CLOSE(콘솔 창 닫기)·CTRL_LOGOFF·CTRL_SHUTDOWN 을 거부할 수 없다 — on_exit(이름)을 부르고 True
  (Ctrl+C·Break 는 파이썬 SIGINT 처리기로 넘긴다 — False). 창 닫기는 약 5초 안에 끝나야 한다.
- 로그오프·윈도우 종료: user32.dll 이 올라온 프로세스(이 파이썬은 처음부터 올라와 있다)에는 콘솔 신호가 오지 않는다(윈도우 규칙) —
  숨은 최상위 창으로 WM_QUERYENDSESSION(거부하지 않음)·WM_ENDSESSION(wParam 참이면 on_exit("WM_ENDSESSION"))을 받는다.
- 리눅스·맥에서는 아무것도 하지 않는다(install 이 None) — 같은 처리는 app 의 SIGINT·SIGTERM 경로(resume.intentional_exit).
on_exit 는 처리기 스레드에서 불린다 — 끝나면 os._exit(0)(종료 코드 0 = 감시 프로그램이 다시 켜지 않음)까지 on_exit 쪽이 정한다.
"""
from __future__ import annotations

import sys
import threading
from typing import Any, Callable, Dict, Optional

CTRL_C, CTRL_BREAK, CTRL_CLOSE, CTRL_LOGOFF, CTRL_SHUTDOWN = 0, 1, 2, 5, 6
CTRL_NAMES = {CTRL_CLOSE: "CTRL_CLOSE", CTRL_LOGOFF: "CTRL_LOGOFF", CTRL_SHUTDOWN: "CTRL_SHUTDOWN"}
WM_QUERYENDSESSION, WM_ENDSESSION = 0x0011, 0x0016
WM_DESTROY, WM_CLOSE = 0x0002, 0x0010
CLASS_NAME = "Ch12V2Session"


def is_windows() -> bool:
    return sys.platform == "win32"


def user32_loaded() -> Optional[bool]:
    """user32.dll 이 올라와 있으면 로그오프·윈도우 종료 신호(CTRL_LOGOFF·CTRL_SHUTDOWN)를 받지 못한다(창으로 받는다)."""
    if not is_windows():
        return None
    try:
        import ctypes
        k = ctypes.WinDLL("kernel32")
        k.GetModuleHandleW.restype = ctypes.c_void_p
        return bool(k.GetModuleHandleW("user32.dll"))
    except Exception:
        return None


class WinShell:
    """콘솔 처리기 + 숨은 창. on_exit(이름) — 한 번만 부른다."""

    def __init__(self, on_exit: Callable[[str], Any], log: Optional[Callable[[str], None]] = None) -> None:
        self.on_exit = on_exit
        self.log = log or (lambda m: None)
        self._fired = threading.Lock()
        self._ctrl_ref = None
        self._wndproc = None
        self._session: Optional[threading.Thread] = None
        self._hwnd = None

    def fire(self, why: str) -> None:
        if not self._fired.acquire(blocking=False):
            return
        try:
            self.on_exit(why)
        except Exception as e:
            self.log(f"종료 처리 오류: {e!r}")

    # ── 콘솔 신호 ──
    def install_ctrl_handler(self) -> bool:
        if not is_windows():
            return False
        try:
            import ctypes
            from ctypes import wintypes
            proto = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.DWORD)

            def handler(ctrl):
                if ctrl in CTRL_NAMES:
                    self.fire(CTRL_NAMES[ctrl])
                    return True
                return False                               # Ctrl+C·Break 는 파이썬 처리기(SIGINT)로
            self._ctrl_ref = proto(handler)
            ctypes.WinDLL("kernel32").SetConsoleCtrlHandler(self._ctrl_ref, True)
            return True
        except Exception as e:
            self.log(f"콘솔 닫기 처리기 설치 실패: {e!r}")
            return False

    # ── 로그오프·윈도우 종료(숨은 창) ──
    def start_session_window(self) -> Optional[int]:
        if not is_windows():
            return None
        ready = threading.Event()
        box: Dict[str, Any] = {}
        t = threading.Thread(target=self._session_loop, args=(ready, box), name="session-watch", daemon=True)
        t.start()
        ready.wait(3.0)
        self._session, self._hwnd = t, box.get("hwnd")
        return self._hwnd

    def stop(self) -> None:
        """숨은 창 닫기(WM_CLOSE → WM_DESTROY → 메시지 루프 끝) — 정상 종료에서 스레드를 남기지 않게."""
        if self._session is None:
            return
        if self._hwnd:
            import ctypes
            from ctypes import wintypes
            u = ctypes.WinDLL("user32")
            u.PostMessageW.argtypes = [wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM]
            u.PostMessageW(self._hwnd, WM_CLOSE, 0, 0)
        self._session.join(2.0)
        self._session = self._hwnd = None

    def _session_loop(self, ready: threading.Event, box: Dict[str, Any]) -> None:
        try:
            import ctypes
            from ctypes import wintypes
            u = ctypes.WinDLL("user32", use_last_error=True)
            k = ctypes.WinDLL("kernel32", use_last_error=True)
            LRESULT = ctypes.c_ssize_t
            WNDPROC = ctypes.WINFUNCTYPE(LRESULT, wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM)

            class WNDCLASSW(ctypes.Structure):
                _fields_ = [("style", wintypes.UINT), ("lpfnWndProc", WNDPROC), ("cbClsExtra", ctypes.c_int),
                            ("cbWndExtra", ctypes.c_int), ("hInstance", wintypes.HINSTANCE), ("hIcon", wintypes.HICON),
                            ("hCursor", wintypes.HANDLE), ("hbrBackground", wintypes.HBRUSH),
                            ("lpszMenuName", wintypes.LPCWSTR), ("lpszClassName", wintypes.LPCWSTR)]
            u.DefWindowProcW.restype = LRESULT
            u.DefWindowProcW.argtypes = [wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM]
            u.CreateWindowExW.restype = wintypes.HWND
            u.CreateWindowExW.argtypes = [wintypes.DWORD, wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD, ctypes.c_int,
                                          ctypes.c_int, ctypes.c_int, ctypes.c_int, wintypes.HWND, wintypes.HMENU,
                                          wintypes.HINSTANCE, wintypes.LPVOID]
            k.GetModuleHandleW.restype = wintypes.HMODULE
            u.UnregisterClassW.argtypes = [wintypes.LPCWSTR, wintypes.HINSTANCE]

            def proc(h, msg, wp, lp):
                if msg == WM_QUERYENDSESSION:
                    self.log("로그오프·윈도우 종료 요청(WM_QUERYENDSESSION) 받음")
                    return 1                               # 거부하지 않는다(결정 4 — 파워 출력만 끄고 끝)
                if msg == WM_ENDSESSION:
                    if wp:
                        self.fire("WM_ENDSESSION")
                    return 0
                if msg == WM_DESTROY:
                    u.PostQuitMessage(0)
                    return 0
                return u.DefWindowProcW(h, msg, wp, lp)
            self._wndproc = WNDPROC(proc)
            wc = WNDCLASSW()
            wc.lpfnWndProc, wc.lpszClassName, wc.hInstance = self._wndproc, CLASS_NAME, k.GetModuleHandleW(None)
            u.RegisterClassW(ctypes.byref(wc))
            hwnd = u.CreateWindowExW(0, CLASS_NAME, "ch12-v2 session", 0, 0, 0, 0, 0, None, None, wc.hInstance, None)
            box["hwnd"] = hwnd
            ready.set()
            if not hwnd:
                self.log(f"로그오프·윈도우 종료 감시 창을 만들지 못함({ctypes.get_last_error()})")
                return
            self.log("로그오프·윈도우 종료 감시 창(WM_ENDSESSION) 준비")
            msg = wintypes.MSG()
            while u.GetMessageW(ctypes.byref(msg), None, 0, 0) > 0:
                u.TranslateMessage(ctypes.byref(msg))
                u.DispatchMessageW(ctypes.byref(msg))
            u.UnregisterClassW(CLASS_NAME, wc.hInstance)
        except Exception as e:
            ready.set()
            self.log(f"로그오프·윈도우 종료 감시 실패: {e!r}")


def install(on_exit: Callable[[str], Any], log: Optional[Callable[[str], None]] = None) -> Optional[WinShell]:
    """윈도우면 콘솔 처리기·숨은 창을 설치한 WinShell, 아니면 None."""
    if not is_windows():
        return None
    sh = WinShell(on_exit, log)
    sh.install_ctrl_handler()
    sh.start_session_window()
    log and log(f"user32 로드됨: {user32_loaded()} (참이면 로그오프·종료는 숨은 창으로 받음)")
    return sh

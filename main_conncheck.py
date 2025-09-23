# main_conncheck.py
# -*- coding: utf-8 -*-
import sys, asyncio, traceback, contextlib
from typing import Any, Callable, Coroutine
from datetime import datetime
from pathlib import Path
from collections import deque
from typing import Deque, Optional
from typing import Optional, TypedDict, Mapping, Any, Coroutine, Callable, Literal, Sequence, Deque, cast
from PySide6.QtWidgets import QApplication, QWidget, QMessageBox, QPlainTextEdit, QStackedWidget
from PySide6.QtGui import QTextCursor, QCloseEvent
from PySide6.QtCore import QCoreApplication
from qasync import QEventLoop

# ✅ 기존 UI 그대로 사용
from ui.main_window import Ui_Form

# ✅ 필요한 장비만 최소 구성 (연결 확인 대상)
from device.plc import AsyncFaduinoPLC
from device.mfc import AsyncMFC
from device.ig import AsyncIG
from device.rf_pulse import RFPulseAsync

# ──────────────────────────────────────────────────────────────────────
# 메인 윈도우 (원래 구조 유지: 로거/버튼훅/백그라운드태스크/프리플라이트 등)
# ──────────────────────────────────────────────────────────────────────
class MainWindow(QWidget):
    def __init__(self, loop: asyncio.AbstractEventLoop | None = None):
        super().__init__()
        self.ui = Ui_Form()
        self.ui.setupUi(self)

        self._loop = loop or asyncio.get_event_loop()
        self._bg_tasks: list[asyncio.Task] = []
        self._bg_started: bool = False
        self._checking: bool = False
        self._shutdown_called: bool = False

        self._stack = self.ui.stackedWidget
        self._pages = {
            "pc":  self.ui.page_3,
            "ch1": self.ui.page,
            "ch2": self.ui.page_2,
        }
        self._switch_page("ch2")   # ← 이제 호출

        # ── 로그 파일 경로 세팅 (원본과 유사: NAS → 로컬 폴백)
        nas_path = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs")
        local_fallback = Path.cwd() / "_Logs_local"
        try:
            nas_path.mkdir(parents=True, exist_ok=True)
            self._log_dir = nas_path
        except Exception:
            local_fallback.mkdir(parents=True, exist_ok=True)
            self._log_dir = local_fallback
            self._soon(self.ui.ch2_logMessage_edit.appendPlainText, f"[Logger] NAS 접근 실패 → 로컬 폴백: {self._log_dir}")

        self._log_file_path: Path | None = None
        self._prestart_buf: Deque[str] = deque(maxlen=1000)

        # ── 장비 인스턴스 (필요 최소)
        def _plc_log(fmt, *args):
            try:
                msg = (fmt % args) if args else str(fmt)
            except Exception:
                msg = str(fmt)
            self.append_log("PLC", msg)

        self.plc = AsyncFaduinoPLC(logger=_plc_log)
        self.mfc = AsyncMFC()
        self.ig = AsyncIG()
        self.rfpulse = RFPulseAsync()

        # 이 리스트만 바꾸면 “연결 확인 대상”이 바뀜
        self._need_devices: list[tuple[str, object, Callable[[], Coroutine[Any, Any, Any]] | None]] = [
            ("PLC", self.plc, self.plc.connect),  # PLC는 connect()
            ("MFC", self.mfc, self.mfc.start),    # 나머지는 start()
            ("IG", self.ig, self.ig.start),
            ("RFPulse", self.rfpulse, self.rfpulse.start),
        ]

        # ── UI 훅 (원래 구조 유지: Start/Stop 재사용)
        self._connect_ui_signals()

        # ── 로그창 라인 제한 (원래처럼)
        self.ui.ch2_logMessage_edit.setMaximumBlockCount(2000)

        # ── 예외/Qt 로그 훅 (원래처럼 로그창으로 수집)
        self._install_exception_hooks()
        from PySide6.QtCore import qInstallMessageHandler, QtMsgType
        def _qt_msg_handler(mode, context, message):
            tag = {QtMsgType.QtDebugMsg: "QtDebug", QtMsgType.QtInfoMsg: "QtInfo",
                   QtMsgType.QtWarningMsg: "QtWarn", QtMsgType.QtCriticalMsg: "QtCrit",
                   QtMsgType.QtFatalMsg: "QtFatal"}.get(mode, "Qt")
            self.append_log(tag, message)
        qInstallMessageHandler(_qt_msg_handler)

        # 초기 버튼 상태
        self._on_process_status_changed(False)
        self.append_log("MAIN", "연결 확인 전용 모드 준비 완료.")

    # ────────────────────── UI/시그널 ──────────────────────
    def _connect_ui_signals(self):
        self.ui.ch2_Start_button.clicked.connect(self._handle_start_clicked)
        self.ui.ch2_Stop_button.clicked.connect(self._handle_stop_clicked)

    def _on_process_status_changed(self, running: bool):
        # checking 중에는 Start 비활성화, Stop 활성화
        self.ui.ch2_Start_button.setEnabled(not running)
        self.ui.ch2_Stop_button.setEnabled(True)

    # ────────────────────── Start/Stop ──────────────────────
    def _handle_start_clicked(self, _checked: bool = False):
        # 1) 새 로그 파일(세션) 준비
        if not self._log_file_path:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            self._log_file_path = self._log_dir / f"{ts}.txt"
            if self._prestart_buf:
                buf_copy = list(self._prestart_buf); self._prestart_buf.clear()
                self._spawn_detached(self._dump_prestart_buf_async(self._log_file_path, buf_copy))
            self.append_log("Logger", f"새 로그 파일 시작: {self._log_file_path}")

        self.append_log("MAIN", "=== 'ConnCheck' 시작 (장비 연결만 확인) ===")
        self._spawn_detached(self._run_conncheck(), store=True, name="ConnCheck")

    def _handle_stop_clicked(self, _checked: bool = False):
        self.append_log("MAIN", "정지/정리 요청")
        self._spawn_detached(self._cleanup_devices())

    # ────────────────────── 연결 확인 본체 ──────────────────────
    async def _run_conncheck(self):
        if self._checking:
            self.append_log("MAIN", "이미 확인 중입니다.")
            return

        self._checking = True
        self._on_process_status_changed(True)

        # ⬇️ 생성 지점은 try 바깥에 두고
        stop_evt = asyncio.Event()
        prog_task = asyncio.create_task(self._preflight_progress_log(self._need_devices, stop_evt))

        try:
            self.append_log("DBG", "PF: ensure background…")
            self._ensure_background_started()

            timeout_s = 8.0
            self.append_log("DBG", "PF: awaiting _preflight_connect()")
            ok, failed = await self._preflight_connect(self._need_devices, timeout_s=timeout_s)

            if ok:
                self.append_log("MAIN", "✅ 모든 장비 연결 확인 완료")
                QMessageBox.information(self, "연결 확인", "모든 장비 연결이 확인되었습니다.")
            else:
                self.append_log("MAIN", f"❌ 일부 장비 연결 실패: {', '.join(failed)}")
                QMessageBox.critical(self, "연결 실패", "다음 장비 연결 실패:\n - " + "\n - ".join(failed))

        except asyncio.CancelledError:
            self.append_log("MAIN", "연결 확인 취소됨(Stop)")
            raise
        except Exception as e:
            self.append_log("MAIN", f"연결 확인 중 예외: {e!r}")
        finally:
            # ⬇️ 누수/잔여 로그 방지: 항상 종료·취소·합류
            stop_evt.set()
            if prog_task and not prog_task.done():
                prog_task.cancel()
                with contextlib.suppress(Exception):
                    await prog_task

            self._on_process_status_changed(False)
            self._checking = False


    # ────────────────────── 백그라운드 올리기 ──────────────────────
    def _ensure_task_alive(self, name: str, coro_factory: Callable[[], Coroutine[Any, Any, Any]]):
        # 동일 이름 태스크 중복 방지
        self._bg_tasks = [t for t in self._bg_tasks if t and not t.done()]
        for t in self._bg_tasks:
            try:
                if t.get_name() == name and not t.done():
                    return
            except Exception:
                pass
        self._spawn_detached(coro_factory(), store=True, name=name)

    def _ensure_background_started(self):
        # Starter를 가진 장비는 모두 태스크로 기동
        for name, _dev, starter in self._need_devices:
            if starter:
                self._ensure_task_alive(f"{name}.start", starter)
        self._bg_started = True

    # ────────────────────── 프리플라이트 로직 ──────────────────────
    async def _preflight_connect(self,
                                 need: list[tuple[str, object, Callable[[], Coroutine[Any, Any, Any]] | None]],
                                 timeout_s: float = 8.0) -> tuple[bool, list[str]]:
        async def _wait_device_connected(dev: object, name: str, timeout_s: float) -> bool:
            try:
                t0 = asyncio.get_running_loop().time()
            except RuntimeError:
                t0 = 0.0

            def _sync_check() -> bool:
                v = getattr(dev, "is_connected", None)
                if isinstance(v, bool):
                    return v
                if callable(v):
                    return bool(v())
                return bool(getattr(dev, "_connected", False))

            while True:
                try:
                    ok = await asyncio.to_thread(_sync_check)
                except Exception as e:
                    self.append_log(name, f"is_connected 체크 예외: {e!r}")
                    ok = False

                if ok:
                    self.append_log(name, "연결 성공")
                    return True

                try:
                    now = asyncio.get_running_loop().time()
                except RuntimeError:
                    now = t0 + timeout_s + 1.0

                if now - t0 >= timeout_s:
                    self.append_log(name, "연결 확인 실패(타임아웃)")
                    return False

                await asyncio.sleep(0.2)

        results = await asyncio.gather(
            *[_wait_device_connected(dev, name, timeout_s) for name, dev, _ in need],
            return_exceptions=False
        )
        failed = [name for (name, _dev, _), ok in zip(need, results) if not ok]
        return (len(failed) == 0, failed)

    async def _preflight_progress_log(self, need, stop_evt: asyncio.Event):
        try:
            while not stop_evt.is_set():
                missing = []
                for name, dev, _ in need:
                    try:
                        def _sync():
                            v = getattr(dev, "is_connected", None)
                            if isinstance(v, bool):
                                return v
                            if callable(v):
                                return bool(v())
                            return bool(getattr(dev, "_connected", False))
                        ok = await asyncio.to_thread(_sync)
                        if not ok:
                            missing.append(name)
                    except Exception:
                        missing.append(name)
                txt = ", ".join(missing) if missing else "모두 연결됨"
                self.append_log("MAIN", f"연결 대기 중: {txt}")
                try:
                    await asyncio.wait_for(stop_evt.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
        except Exception as e:
            self.append_log("MAIN", f"프리플라이트 진행 로그 예외: {e!r}")

    # ────────────────────── 정리/종료 ──────────────────────
    async def _cleanup_devices(self):
        # 백그라운드 태스크 취소
        live = [t for t in self._bg_tasks if t and not t.done()]
        for t in live:
            self._loop.call_soon(t.cancel)
        if live:
            await asyncio.gather(*live, return_exceptions=True)
        self._bg_tasks = []
        self._bg_started = False

        # 장치 정리
        tasks = []
        for name, dev, _ in self._need_devices:
            try:
                if hasattr(dev, "cleanup_quick"):
                    tasks.append(dev.cleanup_quick())
                elif hasattr(dev, "cleanup"):
                    tasks.append(dev.cleanup())
            except Exception as e:
                self.append_log(name, f"cleanup 예외: {e!r}")

        # PLC는 close()일 수도 있음
        try:
            if hasattr(self.plc, "close"):
                tasks.append(self.plc.close())
        except Exception as e:
            self.append_log("PLC", f"close 예외: {e!r}")

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self.append_log("MAIN", "정리 완료")
        self._on_process_status_changed(False)   # ← 추가

    # ────────────────────── 로그 유틸 ──────────────────────
    def append_log(self, source: str, msg: str) -> None:
        now_ui = datetime.now().strftime("%H:%M:%S")
        now_file = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line_ui = f"[{now_ui}] [{source}] {msg}"
        line_file = f"[{now_file}] [{source}] {msg}\n"

        # UI
        self._soon(self._append_log_to_ui, line_ui)

        # 파일 없으면 버퍼에
        if not getattr(self, "_log_file_path", None):
            try:
                self._prestart_buf.append(line_file)
            except Exception:
                pass
            return

        # 파일 기록은 to_thread
        self._spawn_detached(self._write_log_line_async(self._log_file_path, line_file))

    def _switch_page(self, key: Literal["pc", "ch1", "ch2"]) -> None:
        """'pc' | 'ch1' | 'ch2' 키로 스택 페이지 전환"""
        page = self._pages.get(key)
        if not page:
            self.append_log("UI", f"페이지 키 '{key}' 를 찾을 수 없습니다.")
            return

        # 스택 전환
        try:
            self._stack.setCurrentWidget(page)
        except Exception as e:
            self.append_log("UI", f"페이지 전환 실패({key}): {e}")
            return

    async def _write_log_line_async(self, path: Path, text: str):
        def _write():
            with open(path, "a", encoding="utf-8") as f:
                f.write(text)
        try:
            await asyncio.to_thread(_write)
        except Exception as e:
            self._soon(self.ui.ch2_logMessage_edit.appendPlainText, f"[Logger] 파일 기록 실패: {e}")

    async def _dump_prestart_buf_async(self, path: Path, lines: list[str]):
        def _write_many():
            with open(path, "a", encoding="utf-8") as f:
                f.writelines(lines)
        try:
            await asyncio.to_thread(_write_many)
        except Exception:
            self._soon(self.ui.ch2_logMessage_edit.appendPlainText, "[Logger] 버퍼 덤프 실패")

    def _append_log_to_ui(self, line: str):
        self.ui.ch2_logMessage_edit.moveCursor(QTextCursor.MoveOperation.End)
        self.ui.ch2_logMessage_edit.insertPlainText(line + "\n")

    # ────────────────────── 공용 유틸(원래와 동일 패턴) ──────────────────────
    def _spawn_detached(self, coro, *, store: bool=False, name: str | None = None):
        loop = self._loop
        def _create():
            t = loop.create_task(coro, name=name)
            if store:
                self._bg_tasks.append(t)
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            loop.call_soon(_create)
        else:
            loop.call_soon_threadsafe(_create)

    def _install_exception_hooks(self) -> None:
        try:
            target_loop = asyncio.get_running_loop()
        except RuntimeError:
            target_loop = self._loop

        def excepthook(exctype, value, tb):
            txt = ''.join(traceback.format_exception(exctype, value, tb)).rstrip()
            self.append_log("EXC", txt)

        def loop_exception_handler(loop_, context):
            exc = context.get("exception")
            msg = context.get("message", "")
            if exc:
                try:
                    txt = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__)).rstrip()
                except Exception:
                    txt = f"{exc!r}"
                self.append_log("Asyncio", txt)
            elif msg:
                self.append_log("Asyncio", msg)

        sys.excepthook = excepthook
        target_loop.set_exception_handler(loop_exception_handler)

    def _soon(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        loop = self._loop
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            loop.call_soon(fn, *args, **kwargs)
        else:
            loop.call_soon_threadsafe(fn, *args, **kwargs)

    # ────────────────────── 종료 훅 (원래 패턴 유지) ──────────────────────
    def closeEvent(self, event: QCloseEvent) -> None:
        self.append_log("MAIN", "창 닫힘 → 빠른 정리")
        self._spawn_detached(self._cleanup_then_quit())
        event.accept()
        super().closeEvent(event)

    async def _cleanup_then_quit(self):
        await self._cleanup_devices()
        await asyncio.sleep(0.2)
        QCoreApplication.quit()

# ────────────────────── 엔트리포인트 (원래 패턴: qasync) ──────────────────────
if __name__ == "__main__":
    app = QApplication(sys.argv)
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)
    w = MainWindow(loop)
    w.show()
    with loop:
        loop.run_forever()

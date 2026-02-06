# util/app_logging.py
# -*- coding: utf-8 -*-
r"""
시스템성 로그 전용(프로그램 종료/크래시/미처리 예외/Warning/Qt 메시지 등) 로깅 유틸
- 기존 공정/PLC/서버 로그는 그대로 두고,
- '현재 저장이 안되고 있는 부분'만 \\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\ERROR 에
  하루 1개 파일로 저장한다.
"""

from __future__ import annotations

import os
import atexit
import asyncio
import faulthandler
import logging
import sys
import threading
import warnings
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Optional


DEFAULT_ERROR_ROOT = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\ERROR")
_DEFAULT_LOGGER_NAME: Optional[str] = None

_FAULT_LOCK = threading.Lock()
_FAULT_ENABLED = False


class _FaultTimestampWriter:
    """
    faulthandler 출력은 logging formatter를 안 타므로,
    'Windows fatal exception:' 같은 덤프 시작 지점에 타임스탬프 헤더를 삽입한다.
    """
    def __init__(self, fp):
        self._fp = fp
        self._lock = threading.Lock()

    def write(self, s: str):
        with self._lock:
            # 덤프 시작을 감지하면 타임스탬프 헤더를 먼저 기록
            if ("Windows fatal exception:" in s) or ("Fatal Python error:" in s):
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                self._fp.write(f"\n{ts} [FAULTHANDLER] ===== BEGIN DUMP =====\n")
            self._fp.write(s)
            self._fp.flush()
        return len(s)

    def flush(self):
        with self._lock:
            self._fp.flush()

    def close(self):
        with self._lock:
            self._fp.close()

    # faulthandler가 다른 속성을 찾을 수도 있어서 원본 fp로 위임
    def __getattr__(self, name):
        return getattr(self._fp, name)


def _enable_faulthandler_once(logger: logging.Logger, file_path: Path) -> None:
    global _FAULT_ENABLED
    with _FAULT_LOCK:
        if _FAULT_ENABLED:
            return

        raw_fp = open(file_path, "a", encoding="utf-8", buffering=1)
        faulthandler.enable(file=raw_fp, all_threads=True)

        # logger 객체에 붙여 GC 방지 + 종료 시 닫기
        setattr(logger, "_vanam_fault_fp", raw_fp)

        _FAULT_ENABLED = True


@dataclass(frozen=True)
class LogPaths:
    root: Path
    daily_log: Path


def _safe_mkdir(p: Path) -> Path:
    try:
        p.mkdir(parents=True, exist_ok=True)
        return p
    except Exception:
        # UNC 실패 시 로컬 폴백
        fallback = Path.cwd() / "Logs" / "ERROR"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


def _build_paths(app_name: str, root: Path) -> LogPaths:
    root = _safe_mkdir(root)
    d = datetime.now().strftime("%Y%m%d")
    daily_log = root / f"{app_name}_{d}.log"
    return LogPaths(root=root, daily_log=daily_log)


class _DailyFileHandler(logging.Handler):
    """
    하루 1개 파일을 유지하는 핸들러.
    - 파일명에 날짜가 들어가며, 날짜가 바뀌면 자동으로 새 파일로 reopen.
    """
    def __init__(self, app_name: str, root: Path, level: int = logging.INFO, encoding: str = "utf-8"):
        super().__init__(level=level)
        self._app_name = app_name
        self._root = root
        self._encoding = encoding
        self._cur_date: date = date.today()
        self._stream = None
        self._write_lock = threading.Lock()   # ✅ 추가
        self._paths = _build_paths(app_name, root)
        self._open_for_today()

    @property
    def current_path(self) -> Path:
        return self._paths.daily_log

    def _open_for_today(self) -> None:
        self._paths = _build_paths(self._app_name, self._root)
        try:
            self._stream = open(self._paths.daily_log, "a", encoding=self._encoding, buffering=1)
        except Exception:
            # 혹시 UNC가 순간 끊겼으면 로컬 폴백
            self._root = _safe_mkdir(Path.cwd() / "Logs" / "ERROR")
            self._paths = _build_paths(self._app_name, self._root)
            self._stream = open(self._paths.daily_log, "a", encoding=self._encoding, buffering=1)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            with self._write_lock:            # ✅ 추가
                today = date.today()
                if today != self._cur_date:
                    self._cur_date = today
                    try:
                        if self._stream:
                            self._stream.close()
                    except Exception:
                        pass
                    self._open_for_today()

                msg = self.format(record)
                self._stream.write(msg + "\n")
        except Exception:
            # 로깅 중 예외는 절대 앱을 죽이면 안 됨
            pass

    def close(self) -> None:
        try:
            if self._stream:
                self._stream.close()
        except Exception:
            pass
        super().close()


def setup_app_logging(
    app_name: str = "CH_1_2_program",
    root: Path = DEFAULT_ERROR_ROOT,
    file_level: int = logging.INFO,     # ✅ 파일에도 INFO 저장
    console_level: int = logging.INFO,
    enable_console: bool = False,
) -> logging.Logger:
    """
    시스템성 로그 전용 로거 초기화 (하루 1개 파일)
    - 파일: INFO~ (시작/종료/Warning/Crash 모두 한 파일에)
    - 콘솔: 개발 편의용(원하면 끌 수 있음)
    """
    global _DEFAULT_LOGGER_NAME
    _DEFAULT_LOGGER_NAME = app_name

    logger = logging.getLogger(app_name)

    # 중복 초기화 방지
    if getattr(logger, "_vanam_syslog_ready", False):
        return logger

    logger.setLevel(min(file_level, console_level))
    logger.propagate = False

    fmt = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d [%(levelname)s] [%(name)s] "
            "[pid=%(process)d tid=%(thread)d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 1) 하루 1개 파일 핸들러
    file_handler = _DailyFileHandler(app_name=app_name, root=root, level=file_level)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    # 2) 콘솔 핸들러(개발용)
    if enable_console:
        console = logging.StreamHandler(stream=sys.__stdout__)
        console.setLevel(console_level)
        console.setFormatter(fmt)
        logger.addHandler(console)

    # 3) faulthandler: 같은 파일에 덧붙이기(faulthandler 덤프는 “별도 파일”로 분리)
    try:
        d = datetime.now().strftime("%Y%m%d")
        t = datetime.now().strftime("%H%M%S")
        fault_path = Path(file_handler.current_path.parent) / f"{app_name}_{d}_{t}_pid{os.getpid()}.fault.log"

        _enable_faulthandler_once(logger, fault_path)
        logger.info("faulthandler enabled -> %s (timestamp header)", fault_path)

        # ⬇️ (옵션) 응답없음/멈춤 진단용: 일정 시간마다 스택 덤프
        # 환경변수 VANAM_HANG_DUMP_S=120 같은 식으로 켜기(초 단위)
        try:
            hang_s = float(os.environ.get("VANAM_HANG_DUMP_S", "0") or "0")
            if hang_s > 0:
                faulthandler.dump_traceback_later(hang_s, repeat=True)
                logger.info("hang dump enabled: dump_traceback_later(%ss, repeat=True)", hang_s)
        except Exception:
            logger.exception("hang dump enable failed")
    except Exception:
        logger.exception("faulthandler enable failed")

    # 4) 종료 로그(정상 종료면 반드시 남음)
    def _on_exit():
        try:
            logger.info("process exiting (atexit)")
            try:
                faulthandler.cancel_dump_traceback_later()
            except Exception:
                pass
        except Exception:
            pass

        # ✅ 중요: 먼저 disable
        try:
            faulthandler.disable()
        except Exception:
            pass

        # ✅ 그 다음 파일 close
        try:
            fp = getattr(logger, "_vanam_fault_fp", None)
            if fp:
                fp.close()
        except Exception:
            pass

    atexit.register(_on_exit)

    setattr(logger, "_vanam_syslog_ready", True)
    logger.info("system logging ready. root=%s", _build_paths(app_name, root).root)
    return logger


def get_app_logger(default_name: str = "CH_1_2_program") -> logging.Logger:
    name = _DEFAULT_LOGGER_NAME or default_name
    return logging.getLogger(name)


def install_global_exception_hooks(logger: logging.Logger) -> None:
    """메인 스레드/스레드 미처리 예외 로깅"""
    def _sys_hook(exc_type, exc, tb):
        try:
            logger.critical("UNCAUGHT EXCEPTION (sys.excepthook)", exc_info=(exc_type, exc, tb))
        except Exception:
            pass
        try:
            sys.__excepthook__(exc_type, exc, tb)
        except Exception:
            pass

    sys.excepthook = _sys_hook

    def _thread_hook(args: threading.ExceptHookArgs):
        try:
            logger.critical(
                "UNCAUGHT EXCEPTION (threading.excepthook) thread=%s",
                getattr(args, "thread", None),
                exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
            )
        except Exception:
            pass

    try:
        threading.excepthook = _thread_hook
    except Exception:
        logger.exception("threading.excepthook install failed")

    # Python 객체 소멸자 등에서 나는 "unraisable"도 잡기
    try:
        def _unraisable_hook(unraisable):
            try:
                logger.error(
                    "UNRAISABLE EXCEPTION: %s",
                    getattr(unraisable, "err_msg", None),
                    exc_info=(unraisable.exc_type, unraisable.exc_value, unraisable.exc_traceback),
                )
            except Exception:
                pass
        sys.unraisablehook = _unraisable_hook
    except Exception:
        logger.exception("sys.unraisablehook install failed")


def install_asyncio_exception_logging(loop: asyncio.AbstractEventLoop, logger: logging.Logger) -> None:
    """asyncio loop 예외 + task 크래시 즉시 로깅"""
    def _loop_handler(_loop, context):
        msg = context.get("message")
        exc = context.get("exception")
        try:
            if exc:
                logger.error("ASYNCIO EXCEPTION: %s", msg or "(no message)", exc_info=exc)
            else:
                logger.error("ASYNCIO EXCEPTION: %s | context=%s", msg, context)
        except Exception:
            pass

    try:
        loop.set_exception_handler(_loop_handler)
        logger.info("asyncio exception handler installed")
    except Exception:
        logger.exception("set_exception_handler failed")

    orig_create_task = getattr(loop, "create_task", None)
    if not callable(orig_create_task):
        return

    def _done_callback(task: asyncio.Task):
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("task.exception() failed")
            return
        if exc:
            logger.error("TASK CRASHED", exc_info=exc)

    def _create_task_patched(coro, *args, **kwargs):
        task = orig_create_task(coro, *args, **kwargs)
        try:
            task.add_done_callback(_done_callback)
        except Exception:
            pass
        return task

    try:
        setattr(loop, "create_task", _create_task_patched)
        logger.info("loop.create_task patched for immediate task crash logging")
    except Exception:
        logger.exception("patch loop.create_task failed")


def install_warnings_logging(logger: logging.Logger) -> None:
    """warnings.warn() 류를 파일에 남김"""
    def _showwarning(message, category, filename, lineno, file=None, line=None):
        try:
            logger.warning("PYTHON WARNING %s:%s %s: %s", filename, lineno, category.__name__, message)
        except Exception:
            pass

    try:
        warnings.showwarning = _showwarning
        logger.info("warnings.showwarning hooked")
    except Exception:
        logger.exception("warnings hook failed")


# ====== (추가) 모듈 전역에 보관 ======
_QT_MSG_HANDLER = None
_QT_PREV_MSG_HANDLER = None


def install_qt_message_logging(logger: logging.Logger) -> None:
    """PySide6(Qt) 내부 warning/error/fatal 메시지 로깅"""
    global _QT_MSG_HANDLER, _QT_PREV_MSG_HANDLER

    try:
        from PySide6.QtCore import QtMsgType, qInstallMessageHandler
    except Exception:
        logger.info("Qt message hook skipped (PySide6 not available)")
        return

    level_map = {
        QtMsgType.QtDebugMsg: logging.INFO,
        QtMsgType.QtInfoMsg: logging.INFO,
        QtMsgType.QtWarningMsg: logging.WARNING,
        QtMsgType.QtCriticalMsg: logging.ERROR,
        QtMsgType.QtFatalMsg: logging.CRITICAL,
    }

    def _handler(msg_type, ctx, msg):
        # ✅ 최대한 가볍게, 절대 예외 밖으로 던지지 않기
        try:
            lv = level_map.get(msg_type, logging.INFO)
            where = ""
            try:
                if ctx and getattr(ctx, "file", None):
                    where = f" ({ctx.file}:{ctx.line} {ctx.function})"
            except Exception:
                where = ""
            logger.log(lv, "QT %s%s", msg, where)
        except Exception:
            pass

    try:
        # ✅ GC/종료 타이밍 크래시 방지: 전역에 강하게 보관
        _QT_MSG_HANDLER = _handler
        _QT_PREV_MSG_HANDLER = qInstallMessageHandler(_QT_MSG_HANDLER)
        logger.info("Qt message handler installed")
    except Exception:
        logger.exception("Qt message handler install failed")


def uninstall_qt_message_logging(logger: Optional[logging.Logger] = None) -> None:
    """종료 직전에 Qt message handler를 원복/해제"""
    global _QT_MSG_HANDLER, _QT_PREV_MSG_HANDLER
    try:
        from PySide6.QtCore import qInstallMessageHandler
        # 이전 핸들러가 있으면 복원, 없으면 해제(None)
        qInstallMessageHandler(_QT_PREV_MSG_HANDLER if _QT_PREV_MSG_HANDLER is not None else None)
        if logger:
            logger.info("Qt message handler uninstalled/restored")
    except Exception:
        if logger:
            logger.exception("Qt message handler uninstall failed")
    finally:
        _QT_MSG_HANDLER = None
        _QT_PREV_MSG_HANDLER = None


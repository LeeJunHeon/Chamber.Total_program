# -*- coding: utf-8 -*-
"""app_logging.py

CH1&2 프로그램 전체(호스트/런타임/디바이스/print 포함) 로그를
\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\ERROR 아래에 남기기 위한 전역 로깅 유틸.

포함 기능
 - 표준 logging 설정 (all.log + error.log)
 - stdout/stderr(=print, traceback 출력) 파일로 Tee 저장
 - sys.excepthook / threading.excepthook 로 미처리 예외 파일 저장
 - asyncio(loop) 예외 핸들러 + create_task 패치(작업 예외 즉시 로깅)
 - faulthandler로 네이티브 크래시/강제 덤프 대비

주의
 - 네트워크(UNC) 경로가 일시적으로 끊기면 로그 파일 생성이 실패할 수 있으므로
   로컬 폴백을 자동으로 생성합니다.
"""

from __future__ import annotations

import atexit
import asyncio
import faulthandler
import logging
import os
import sys
import threading
import traceback
from dataclasses import dataclass
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


DEFAULT_ERROR_ROOT = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\ERROR")

# setup_app_logging()에서 설정된 기본 로거 이름
_DEFAULT_LOGGER_NAME: Optional[str] = None


@dataclass(frozen=True)
class LogPaths:
    root: Path
    all_log: Path
    err_log: Path
    fault_log: Path


class _TeeStream:
    """write()를 원본 스트림 + logger로 동시에 보냄 (print 캡처용)"""

    def __init__(self, original, logger: logging.Logger, level: int):
        self._orig = original
        self._logger = logger
        self._level = level
        self._buf = ""

    def write(self, s: str) -> int:
        try:
            if self._orig:
                self._orig.write(s)
        except Exception:
            pass

        # 줄 단위로 logger에 남기기
        self._buf += s
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            line = line.rstrip()
            if line:
                try:
                    self._logger.log(self._level, line)
                except Exception:
                    pass
        return len(s)

    def flush(self) -> None:
        try:
            if self._orig:
                self._orig.flush()
        except Exception:
            pass
        # 남은 버퍼 flush
        if self._buf.strip():
            try:
                self._logger.log(self._level, self._buf.strip())
            except Exception:
                pass
        self._buf = ""


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
    d = datetime.now().strftime("%Y%m%d")
    root = _safe_mkdir(root)

    all_log = root / f"{app_name}_{d}_all.log"
    err_log = root / f"{app_name}_{d}_error.log"
    fault_log = root / f"{app_name}_{d}_fault.log"
    return LogPaths(root=root, all_log=all_log, err_log=err_log, fault_log=fault_log)


def setup_app_logging(
    app_name: str = "CH1&2_program",
    root: Path = DEFAULT_ERROR_ROOT,
    level: int = logging.INFO,
    redirect_stdio: bool = True,
) -> logging.Logger:
    """
    전역 로깅 1회 초기화.
    반환: app logger
    """
    global _DEFAULT_LOGGER_NAME
    _DEFAULT_LOGGER_NAME = app_name

    logger = logging.getLogger(app_name)

    # 이미 초기화된 경우 중복 핸들러 방지
    if getattr(logger, "_vanam_logging_ready", False):
        return logger

    paths = _build_paths(app_name, root)

    logger.setLevel(level)
    logger.propagate = False  # 중복 방지

    fmt = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 1) all log (INFO~)
    all_handler = RotatingFileHandler(
        paths.all_log,
        maxBytes=25 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8",
    )
    all_handler.setLevel(level)
    all_handler.setFormatter(fmt)
    logger.addHandler(all_handler)

    # 2) error log (ERROR~)
    err_handler = RotatingFileHandler(
        paths.err_log,
        maxBytes=10 * 1024 * 1024,
        backupCount=20,
        encoding="utf-8",
    )
    err_handler.setLevel(logging.ERROR)
    err_handler.setFormatter(fmt)
    logger.addHandler(err_handler)

    # 3) console (개발 편의)
    console = logging.StreamHandler(stream=sys.__stdout__)
    console.setLevel(level)
    console.setFormatter(fmt)
    logger.addHandler(console)

    # 4) faulthandler (네이티브 크래시/덤프 대비)
    try:
        fault_fp = open(paths.fault_log, "a", encoding="utf-8", buffering=1)
        faulthandler.enable(file=fault_fp, all_threads=True)
        logger.info("faulthandler enabled -> %s", paths.fault_log)
    except Exception:
        logger.exception("faulthandler enable failed")

    # 5) print/stdout/stderr 캡처
    if redirect_stdio:
        try:
            sys.stdout = _TeeStream(sys.__stdout__, logger, logging.INFO)
            sys.stderr = _TeeStream(sys.__stderr__, logger, logging.ERROR)
            logger.info("stdio redirected (tee) -> logger")
        except Exception:
            logger.exception("stdio redirect failed")

    # 6) 종료 로그
    def _on_exit():
        try:
            logger.info("process exiting (atexit)")
        except Exception:
            pass

    atexit.register(_on_exit)

    # mark
    setattr(logger, "_vanam_logging_ready", True)
    logger.info("global logging ready. root=%s", paths.root)
    return logger


def get_app_logger(default_name: str = "CH1&2_program") -> logging.Logger:
    """
    setup_app_logging() 이후에는 해당 app_name 로거를,
    그 전에는 default_name 로거를 반환.
    """
    name = _DEFAULT_LOGGER_NAME or default_name
    return logging.getLogger(name)


def install_global_exception_hooks(logger: logging.Logger) -> None:
    """메인 스레드/스레드 미처리 예외를 파일로 남김."""
    def _sys_hook(exc_type, exc, tb):
        try:
            logger.critical("UNCAUGHT EXCEPTION (sys.excepthook)", exc_info=(exc_type, exc, tb))
        except Exception:
            pass
        # 기본 훅도 호출 (디버그 환경에서는 도움이 됨)
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
        threading.excepthook = _thread_hook  # py>=3.8
    except Exception:
        logger.exception("threading.excepthook install failed")


def install_asyncio_exception_logging(loop: asyncio.AbstractEventLoop, logger: logging.Logger) -> None:
    """
    asyncio 예외를 즉시 로깅 + loop.create_task를 패치해서 Task 예외를 바로 남김.
    (qasync QEventLoop에서도 동작)
    """
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

    # create_task patch (Task exception 즉시 수집)
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
            logger.error("TASK CRASHED: %s", getattr(task, "get_name", lambda: "")(), exc_info=exc)

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


def log_exception(logger: logging.Logger, where: str) -> None:
    """현재 예외를 traceback 포함 로깅."""
    et, ev, tb = sys.exc_info()
    if et is None:
        logger.error("[%s] log_exception called but no active exception", where)
        return
    logger.error("[%s] exception caught", where, exc_info=(et, ev, tb))

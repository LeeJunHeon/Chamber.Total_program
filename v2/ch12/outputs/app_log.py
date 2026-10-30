"""ERROR 일자 로그(시스템성 로그)·faulthandler 파일 — v1 util/app_logging.py 의 파일 출력(LOG-084~088).

v1 그대로
- {LOG_ROOT_DIR}\\ERROR\\CH_1_2_program_YYYYMMDD.log(하루 1개, 날짜가 바뀌면 새 파일), INFO 이상,
  형식 '%(asctime)s.%(msecs)03d [LEVEL] [name] [pid=.. tid=..] msg'(datefmt %Y-%m-%d %H:%M:%S), utf-8 line-buffered
  텍스트 모드(v1 과 같은 open — 윈도우는 \\r\\n). 콘솔 끔.
- 폴백: primary 실패 시 LOCAL_FALLBACK_ROOT\\ERROR, 폴백 중 30 s 마다 primary 복귀 시도(복귀하면
  '# [YYYY-mm-dd HH:MM:SS] primary 로그 경로 복구 → <경로>' 한 줄), 쓰기 실패면 재오픈 1회.
- faulthandler 별도 파일 CH_1_2_program_{YYYYMMDD}_{HHMMSS}_pid{pid}.fault.log(ERROR 폴더), 모든 스레드,
  'Windows fatal exception:'/'Fatal Python error:' 앞에 '<ts> [FAULTHANDLER] ===== BEGIN DUMP =====' 머리, 환경변수
  VANAM_HANG_DUMP_S>0 이면 그 주기로 반복 덤프, atexit 에 'process exiting (atexit)' 후 disable·close.
- 남는 것: 미처리 예외(sys·threading CRITICAL), unraisable, warnings, SIGINT/SIGTERM/SIGBREAK('PROCESS SIGNAL RECEIVED'),
  방송 로그 중 WARNING 이상(classify_log_level — v1 main._log_global 이 쓰던 순수 함수).
v2 차이: 파일 쓰기는 QueueHandler → QueueListener 스레드 하나(부른 스레드가 디스크를 기다리지 않음 — 형식·순서는 같다).
asyncio·Qt 메시지 훅은 v2 에 asyncio·Qt 가 없어 해당 없음(태스크 크래시 중복 억제도 같은 이유 — 결정 목록 후보).

부르는 법
  logger = setup_app_logging(cfg)            # 프로그램 시작 때 한 번(설치 훅 포함). 종료 때 shutdown_app_logging()
  level = classify_log_level(source, msg)    # 방송 줄 판정 → WARNING 이상이면 logger.log(level, "[%s] %s", source, msg)
"""
from __future__ import annotations

import atexit
import faulthandler
import logging
import logging.handlers
import os
import queue
import re
import signal
import sys
import threading
import time
import warnings
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from v2.ch12.outputs.paths import Paths

APP_NAME = "CH_1_2_program"
FORMAT = "%(asctime)s.%(msecs)03d [%(levelname)s] [%(name)s] [pid=%(process)d tid=%(thread)d] %(message)s"
DATEFMT = "%Y-%m-%d %H:%M:%S"


class DailyFileHandler(logging.Handler):
    """하루 1개 파일(v1 _DailyFileHandler 그대로 — util/app_logging.py:124-240)."""
    RETRY_PRIMARY_EVERY_S = 30.0

    def __init__(self, app_name: str, root: Path, fallback_root: Path, level: int = logging.INFO,
                 encoding: str = "utf-8", today: Callable[[], date] = date.today,
                 now: Callable[[], datetime] = datetime.now, monotonic: Callable[[], float] = time.monotonic) -> None:
        super().__init__(level=level)
        self._app_name = app_name
        self._primary_root = Path(root)
        self._fallback_root = Path(fallback_root)
        self._encoding = encoding
        self._today = today
        self._now = now
        self._mono = monotonic
        self._cur_date: date = today()
        self._stream = None
        self._cur_path: Optional[Path] = None
        self._using_fallback = False
        self._last_primary_try = 0.0
        self._write_lock = threading.Lock()
        self._open_stream()

    @property
    def current_path(self) -> Path:
        if self._cur_path is not None:
            return self._cur_path
        return self._fallback_root / f"{self._app_name}_{self._now():%Y%m%d}.log"

    def _close_stream(self) -> None:
        s, self._stream = self._stream, None
        if s is not None:
            try:
                s.flush()
            except Exception:
                pass
            try:
                s.close()
            except Exception:
                pass

    def _open_stream(self) -> None:
        name = f"{self._app_name}_{self._now():%Y%m%d}.log"
        for root, is_fb in ((self._primary_root, False), (self._fallback_root, True)):
            try:
                root.mkdir(parents=True, exist_ok=True)
                p = root / name
                self._stream = open(p, "a", encoding=self._encoding, buffering=1)
                self._cur_path = p
                self._using_fallback = is_fb
                if is_fb:
                    self._last_primary_try = self._mono()
                return
            except Exception:
                continue
        self._stream = None
        self._using_fallback = True
        self._last_primary_try = self._mono()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            with self._write_lock:
                today = self._today()
                if today != self._cur_date:
                    self._cur_date = today
                    self._close_stream()
                    self._open_stream()
                if self._using_fallback and (self._mono() - self._last_primary_try) >= self.RETRY_PRIMARY_EVERY_S:
                    self._last_primary_try = self._mono()
                    self._close_stream()
                    self._open_stream()
                    if (not self._using_fallback) and self._stream is not None:
                        try:
                            self._stream.write(f"# [{self._now():%Y-%m-%d %H:%M:%S}] primary 로그 경로 복구 → {self._cur_path}\n")
                        except Exception:
                            pass
                if self._stream is None:
                    self._open_stream()
                    if self._stream is None:
                        return
                msg = self.format(record)
                try:
                    self._stream.write(msg + "\n")
                    self._stream.flush()
                    return
                except Exception:
                    self._close_stream()
                    self._open_stream()
                    if self._stream is not None:
                        try:
                            self._stream.write(msg + "\n")
                            self._stream.flush()
                        except Exception:
                            pass
        except Exception:
            pass

    def close(self) -> None:
        try:
            self._close_stream()
        except Exception:
            pass
        super().close()


class FaultTimestampWriter:
    """faulthandler 출력에 덤프 시작 머리를 넣는다(util/app_logging.py:51-82)."""

    def __init__(self, fp, now: Callable[[], datetime] = datetime.now) -> None:
        self._fp = fp
        self._now = now
        self._lock = threading.Lock()

    def write(self, s: str):
        with self._lock:
            if ("Windows fatal exception:" in s) or ("Fatal Python error:" in s):
                ts = self._now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
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

    def __getattr__(self, name):
        return getattr(self._fp, name)


_TS_TAG_PREFIX_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+(?:\[[^\]]{0,80}\]\s*)?")


def classify_log_level(source: str, msg: str) -> int:
    """출처·본문으로 로그 레벨(v1 util/app_logging.py:398-417 그대로 — EL-043)."""
    src_u = (source or "").upper()
    msg_s = (msg or "").strip()
    body = _TS_TAG_PREFIX_RE.sub("", msg_s, count=1)
    body_u = body.upper()
    if (src_u.startswith("ERROR") or src_u.startswith("ERR") or "ERROR" in src_u or body.startswith("❌")
            or body_u.startswith("ERROR")):
        return logging.ERROR
    if src_u.startswith("WARN") or "WARN" in src_u or body.startswith("⚠") or body_u.startswith("WARN"):
        return logging.WARNING
    return logging.INFO


_STATE: dict = {}


def setup_app_logging(cfg: Optional[Mapping[str, Any]] = None, app_name: str = APP_NAME, *,
                      install_hooks: bool = True, now: Callable[[], datetime] = datetime.now) -> logging.Logger:
    """시스템성 로거(하루 1개 파일 + faulthandler). 두 번 불러도 한 번만."""
    logger = logging.getLogger(app_name)
    if getattr(logger, "_vanam_syslog_ready", False):
        return logger
    paths = Paths(cfg)
    root, fb = paths.error_dirs()
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = DailyFileHandler(app_name, root, fb, now=now)
    handler.setFormatter(logging.Formatter(fmt=FORMAT, datefmt=DATEFMT))
    q: "queue.Queue" = queue.Queue(-1)
    qh = logging.handlers.QueueHandler(q)
    listener = logging.handlers.QueueListener(q, handler, respect_handler_level=True)
    listener.start()
    logger.addHandler(qh)
    _STATE.update(listener=listener, handler=handler, logger=logger)
    try:
        d, t = now().strftime("%Y%m%d"), now().strftime("%H%M%S")
        fault_path = Path(handler.current_path.parent) / f"{app_name}_{d}_{t}_pid{os.getpid()}.fault.log"
        raw = open(fault_path, "a", encoding="utf-8", buffering=1)
        ffp = FaultTimestampWriter(raw, now)
        faulthandler.enable(file=ffp, all_threads=True)
        _STATE["fault_fp"] = ffp
        logger.info("faulthandler enabled -> %s", fault_path)
        hang_s = float(os.environ.get("VANAM_HANG_DUMP_S", "0") or "0")
        if hang_s > 0:
            faulthandler.dump_traceback_later(hang_s, repeat=True)
            logger.info("hang dump enabled: dump_traceback_later(%ss, repeat=True)", hang_s)
    except Exception:
        logger.exception("faulthandler enable failed")
    atexit.register(shutdown_app_logging)
    if install_hooks:
        install_global_hooks(logger)
    logger._vanam_syslog_ready = True          # type: ignore[attr-defined]
    logger.info("system logging ready. root=%s", handler.current_path.parent)
    return logger


def shutdown_app_logging() -> None:
    logger = _STATE.get("logger")
    if logger is None:
        return
    try:
        logger.info("process exiting (atexit)")
        faulthandler.cancel_dump_traceback_later()
    except Exception:
        pass
    try:
        if _STATE.get("fault_fp") is not None:
            faulthandler.disable()
            _STATE["fault_fp"].close()
    except Exception:
        pass
    try:
        _STATE["listener"].stop()
        _STATE["handler"].close()
        logger.removeHandler(next(h for h in logger.handlers if isinstance(h, logging.handlers.QueueHandler)))
    except Exception:
        pass
    logger._vanam_syslog_ready = False         # type: ignore[attr-defined]
    _STATE.clear()


def install_global_hooks(logger: logging.Logger) -> None:
    """미처리 예외·unraisable·warnings·종료 신호(util/app_logging.py:347-389,543-651)."""
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

    def _thread_hook(args):
        try:
            logger.critical("UNCAUGHT EXCEPTION (threading.excepthook) thread=%s", getattr(args, "thread", None),
                            exc_info=(args.exc_type, args.exc_value, args.exc_traceback))
        except Exception:
            pass
    threading.excepthook = _thread_hook

    def _unraisable_hook(u):
        try:
            logger.error("UNRAISABLE EXCEPTION: %s", getattr(u, "err_msg", None),
                         exc_info=(u.exc_type, u.exc_value, u.exc_traceback))
        except Exception:
            pass
    sys.unraisablehook = _unraisable_hook

    def _showwarning(message, category, filename, lineno, file=None, line=None):
        try:
            logger.warning("PYTHON WARNING %s:%s %s: %s", filename, lineno, category.__name__, message)
        except Exception:
            pass
    warnings.showwarning = _showwarning
    logger.info("warnings.showwarning hooked")
    if threading.current_thread() is not threading.main_thread():
        return
    for sig_name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            prev = signal.getsignal(sig)

            def _handler(signum, frame, _n=sig_name, _prev=prev):
                try:
                    logger.critical("PROCESS SIGNAL RECEIVED: %s (%s)", _n, signum)
                except Exception:
                    pass
                try:
                    if callable(_prev):
                        _prev(signum, frame)
                except Exception:
                    pass
            signal.signal(sig, _handler)
            logger.info("signal handler installed: %s", sig_name)
        except Exception:
            logger.exception("signal handler install failed: %s", sig_name)

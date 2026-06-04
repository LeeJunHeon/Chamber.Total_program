# -*- coding: utf-8 -*-
"""
util/log_hub.py
공용 keep-handle 로깅 모듈

목표
- 파일을 "한 번 열고(핸들 유지)" 계속 write+flush 해서 open/close 반복 제거
- NAS(UNC) 쓰기 실패/잠금/일시 끊김 발생 시 로컬 폴백으로 자동 전환
- (옵션) 폴백 상태에서 일정 주기마다 NAS 재시도

주의
- 이 모듈은 '파일 IO 로직을 한 곳에서 관리'하기 위한 것이고,
  출력 파일 자체(PLC 하루 1개, 서버 하루 1개, 공정 run 1개 등)는 목적에 맞게 여러 개로 유지하는 게 정상입니다.
"""

from __future__ import annotations

import os
import csv
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence


def _safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _is_empty_file(p: Path) -> bool:
    try:
        return (not p.exists()) or (p.stat().st_size == 0)
    except Exception:
        # stat 실패(네트워크/권한 등) 시, 헤더 중복 방지를 위해 "비어있지 않다"로 취급
        return False


@dataclass
class FallbackState:
    using_fallback: bool = False
    last_error: Optional[Exception] = None
    last_primary_try_monotonic: float = 0.0
    switched_to_fallback_once: bool = False  # write 중 1회 전환 플래그(소비형)


class DailyCsvListAppender:
    """
    - 하루 1개 CSV에 list(row) append (csv.writer)
    - keep-handle 유지
    - primary_dir(NAS) 실패 시 fallback_dir(로컬)로 자동 전환
    - fallback 상태면 retry_primary_every_s 마다 NAS 재시도
    """

    def __init__(
        self,
        *,
        primary_dir: Path,
        fallback_dir: Path,
        filename_builder: Callable[[datetime], str],
        encoding: str = "utf-8-sig",
        retry_primary_every_s: float = 10.0,
    ) -> None:
        self._primary_dir = Path(primary_dir)
        self._fallback_dir = Path(fallback_dir)
        self._filename_builder = filename_builder
        self._encoding = encoding
        self._retry_s = float(retry_primary_every_s)

        self._lock = threading.RLock()
        self._fp = None
        self._writer = None
        self._cur_path: Optional[Path] = None
        self._cur_header: Optional[Sequence[str]] = None
        self.state = FallbackState()

    @property
    def current_path(self) -> Optional[Path]:
        return self._cur_path

    def consume_switched_flag(self) -> bool:
        """이번 write에서 NAS->LOCAL 폴백이 발생했는지 1회성으로 알려줌."""
        with self._lock:
            if self.state.switched_to_fallback_once:
                self.state.switched_to_fallback_once = False
                return True
            return False

    def close(self) -> None:
        with self._lock:
            fp = self._fp
            self._fp = None       # ✅ 먼저 None으로 (GC가 invalidated 핸들에 접근하는 것 방지)
            self._writer = None
            try:
                if fp is not None:
                    fp.flush()
                    fp.close()
            except Exception:
                pass
            self._cur_path = None
            self._cur_header = None

    def _path_for(self, dt: datetime, *, use_fallback: bool) -> Path:
        base = self._fallback_dir if use_fallback else self._primary_dir
        return base / self._filename_builder(dt)

    def _open(self, path: Path, header: Sequence[str]) -> None:
        _safe_mkdir(path.parent)
        need_header = _is_empty_file(path)

        self._fp = open(path, "a", encoding=self._encoding, newline="")
        self._writer = csv.writer(self._fp)
        self._cur_path = path
        self._cur_header = list(header)

        if need_header:
            self._writer.writerow(list(header))
            self._fp.flush()

    def _ensure_open(self, dt: datetime, header: Sequence[str]) -> None:
        want_path = self._path_for(dt, use_fallback=self.state.using_fallback)

        if (
            self._fp is None
            or self._writer is None
            or self._cur_path != want_path
            or list(header) != list(self._cur_header or [])
        ):
            self.close()
            self._open(want_path, header)

    def _maybe_retry_primary(self, dt: datetime, header: Sequence[str]) -> None:
        if not self.state.using_fallback:
            return
        if self._retry_s <= 0:
            return

        now = time.monotonic()
        if now - float(self.state.last_primary_try_monotonic) < self._retry_s:
            return

        self.state.last_primary_try_monotonic = now
        primary_path = self._path_for(dt, use_fallback=False)

        try:
            self.close()
            self._open(primary_path, header)
            self.state.using_fallback = False
            self.state.last_error = None
        except Exception as e:
            # 재시도 실패 → 다시 fallback으로 복귀
            self.state.last_error = e
            self.state.using_fallback = True
            self.close()
            self._open(self._path_for(dt, use_fallback=True), header)

    def append_row(self, *, dt: datetime, header: Sequence[str], row: Sequence[str]) -> None:
        with self._lock:
            # fallback 중이면 가끔 NAS로 다시 붙기 시도
            self._maybe_retry_primary(dt, header)

            try:
                self._ensure_open(dt, header)
                self._writer.writerow(list(row))
                self._fp.flush()
            except Exception as e:
                # primary에서 실패하면 fallback으로 1회 전환 후 재시도
                self.state.last_error = e
                if not self.state.using_fallback:
                    self.state.using_fallback = True
                    self.state.switched_to_fallback_once = True

                    self.close()
                    self._open(self._path_for(dt, use_fallback=True), header)

                    self._writer.writerow(list(row))
                    self._fp.flush()
                else:
                    # fallback에서도 실패 → 호출부가 삼키거나 처리
                    raise


class DailyCsvDictAppender:
    """
    - 하루 1개 CSV에 dict(row) append (csv.DictWriter)
    - keep-handle 유지
    - primary_dir 실패 시 fallback_dir로 자동 전환 + (옵션) NAS 재시도
    """

    def __init__(
        self,
        *,
        primary_dir: Path,
        fallback_dir: Path,
        filename_builder: Callable[[datetime], str],
        fieldnames: Sequence[str],
        encoding: str = "utf-8-sig",
        retry_primary_every_s: float = 10.0,
    ) -> None:
        self._primary_dir = Path(primary_dir)
        self._fallback_dir = Path(fallback_dir)
        self._filename_builder = filename_builder
        self._fieldnames = list(fieldnames)
        self._encoding = encoding
        self._retry_s = float(retry_primary_every_s)

        self._lock = threading.RLock()
        self._fp = None
        self._writer: Optional[csv.DictWriter] = None
        self._cur_path: Optional[Path] = None
        self.state = FallbackState()

    @property
    def current_path(self) -> Optional[Path]:
        return self._cur_path

    def consume_switched_flag(self) -> bool:
        with self._lock:
            if self.state.switched_to_fallback_once:
                self.state.switched_to_fallback_once = False
                return True
            return False

    def close(self) -> None:
        with self._lock:
            fp = self._fp
            self._fp = None       # ✅ 먼저 None으로
            self._writer = None
            self._cur_path = None
            try:
                if fp is not None:
                    fp.flush()
                    fp.close()
            except Exception:
                pass

    def _path_for(self, dt: datetime, *, use_fallback: bool) -> Path:
        base = self._fallback_dir if use_fallback else self._primary_dir
        return base / self._filename_builder(dt)

    def _open(self, path: Path) -> None:
        _safe_mkdir(path.parent)
        need_header = _is_empty_file(path)

        self._fp = open(path, "a", encoding=self._encoding, newline="")
        self._writer = csv.DictWriter(self._fp, fieldnames=self._fieldnames)
        self._cur_path = path

        if need_header:
            self._writer.writeheader()
            self._fp.flush()

    def _ensure_open(self, dt: datetime) -> None:
        want_path = self._path_for(dt, use_fallback=self.state.using_fallback)
        if self._fp is None or self._writer is None or self._cur_path != want_path:
            self.close()
            self._open(want_path)

    def _maybe_retry_primary(self, dt: datetime) -> None:
        if not self.state.using_fallback:
            return
        if self._retry_s <= 0:
            return

        now = time.monotonic()
        if now - float(self.state.last_primary_try_monotonic) < self._retry_s:
            return

        self.state.last_primary_try_monotonic = now
        primary_path = self._path_for(dt, use_fallback=False)

        try:
            self.close()
            self._open(primary_path)
            self.state.using_fallback = False
            self.state.last_error = None
        except Exception as e:
            self.state.last_error = e
            self.state.using_fallback = True
            self.close()
            self._open(self._path_for(dt, use_fallback=True))

    def append_row(self, *, dt: datetime, row: Dict[str, Any]) -> None:
        with self._lock:
            self._maybe_retry_primary(dt)

            try:
                self._ensure_open(dt)
                # 누락 키는 빈 값 처리(헤더 고정)
                self._writer.writerow({k: row.get(k, "") for k in self._fieldnames})
                self._fp.flush()
            except Exception as e:
                self.state.last_error = e
                if not self.state.using_fallback:
                    self.state.using_fallback = True
                    self.state.switched_to_fallback_once = True

                    self.close()
                    self._open(self._path_for(dt, use_fallback=True))

                    self._writer.writerow({k: row.get(k, "") for k in self._fieldnames})
                    self._fp.flush()
                else:
                    raise


class SessionTextAppender:
    """
    - 공정 1회 로그(txt) 같은 '세션 파일'에 append
    - keep-handle 유지
    - primary_path 쓰기 실패 시 fallback_dir/<same name> 으로 1회 전환
    """

    def __init__(self, *, fallback_dir: Optional[Path] = None, encoding: str = "utf-8") -> None:
        self._encoding = encoding
        self._fallback_dir = Path(fallback_dir) if fallback_dir else None

        self._lock = threading.RLock()
        self._primary_path: Optional[Path] = None
        self._cur_path: Optional[Path] = None
        self._fp = None
        self.state = FallbackState()

    @property
    def current_path(self) -> Optional[Path]:
        return self._cur_path

    def consume_switched_flag(self) -> bool:
        with self._lock:
            if self.state.switched_to_fallback_once:
                self.state.switched_to_fallback_once = False
                return True
            return False

    def set_primary_path(self, path: Path) -> None:
        with self._lock:
            p = Path(path)
            if self._primary_path != p:
                self.close()
                self._primary_path = p
                self.state = FallbackState()

    def close(self) -> None:
        with self._lock:
            fp = self._fp
            self._fp = None       # ✅ 먼저 None으로
            self._cur_path = None
            try:
                if fp is not None:
                    fp.flush()
                    fp.close()
            except Exception:
                pass

    def _open(self, path: Path) -> None:
        _safe_mkdir(path.parent)
        self._fp = open(path, "a", encoding=self._encoding, newline="")
        self._cur_path = path

    def _fallback_path(self) -> Path:
        if not self._fallback_dir:
            return Path(self._primary_path)  # type: ignore
        return self._fallback_dir / Path(self._primary_path).name  # type: ignore

    def _ensure_open(self) -> None:
        if self._primary_path is None:
            raise RuntimeError("SessionTextAppender.primary_path is not set")

        target = Path(self._primary_path) if not self.state.using_fallback else self._fallback_path()
        if self._fp is None or self._cur_path != target:
            self.close()
            self._open(target)

    def write(self, text: str) -> None:
        with self._lock:
            try:
                self._ensure_open()
                self._fp.write(text)
                self._fp.flush()
            except Exception as e:
                self.state.last_error = e
                if (not self.state.using_fallback) and self._fallback_dir:
                    self.state.using_fallback = True
                    self.state.switched_to_fallback_once = True

                    self.close()
                    self._open(self._fallback_path())

                    self._fp.write(text)
                    self._fp.flush()
                else:
                    raise

class FixedCsvDictAppender:
    """
    고정 파일 1개를 keep-handle로 append 하는 writer.
    - 파일이 없거나 비어있으면 헤더 1회 기록
    - append 할 때마다 flush (필요 시 fsync까지)
    """

    def __init__(self, path: Path, fieldnames: List[str], *, encoding: str = "utf-8-sig", fsync_each_write: bool = False):
        self.path = Path(path)
        self.fieldnames = list(fieldnames)
        self.encoding = encoding
        self.fsync_each_write = bool(fsync_each_write)

        self._fp: Optional[Any] = None
        self._writer: Optional[csv.DictWriter] = None

    def open(self) -> None:
        if self._fp is not None:
            return

        self.path.parent.mkdir(parents=True, exist_ok=True)
        new_file = (not self.path.exists()) or (self.path.stat().st_size == 0)

        self._fp = open(self.path, "a", newline="", encoding=self.encoding)
        self._writer = csv.DictWriter(self._fp, fieldnames=self.fieldnames)

        if new_file:
            self._writer.writeheader()
            self._fp.flush()
            if self.fsync_each_write:
                try:
                    os.fsync(self._fp.fileno())
                except Exception:
                    pass

    def append_row(self, row: Dict[str, Any]) -> None:
        if self._fp is None or self._writer is None:
            self.open()

        assert self._writer is not None and self._fp is not None
        safe_row = {k: row.get(k, "") for k in self.fieldnames}
        self._writer.writerow(safe_row)
        self._fp.flush()
        if self.fsync_each_write:
            try:
                os.fsync(self._fp.fileno())
            except Exception:
                pass

    def close(self) -> None:
        fp = self._fp
        self._fp = None       # ✅ 먼저 None으로
        self._writer = None
        if fp is not None:
            try:
                fp.flush()
            except Exception:
                pass
            try:
                fp.close()
            except Exception:
                pass

    def is_open(self) -> bool:
        return self._fp is not None
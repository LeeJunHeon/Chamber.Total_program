# -*- coding: utf-8 -*-
"""호스트 요청 공정 로그 — 이어쓰기 전 끊긴 줄 보정(_ensure_line_end).
pending · 내 사본 · 메모리 큐 재기록 · NAS 공용 파일 4곳 모두, 끝이 끊긴 파일에 이어 쓰면 새 행이
별도 줄로 남고, 정상/빈/없는 파일은 지금과 같은 바이트가 되는지."""
from __future__ import annotations

import csv
import io
import os
import sys
from pathlib import Path

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from lib import config_common as cfgc                                  # noqa: E402
from util.host_process_log import HostProcessLog, HEADER, N_COLS, _csv_line   # noqa: E402

YMD = "20261022"
BOM = b"\xef\xbb\xbf"
CUT = b"20261022,cut-row-partial"          # 전원 차단으로 줄 중간에서 끊긴 기록(줄바꿈 없음)


def _row(key: str):
    return [f"c{i}" for i in range(N_COLS - 1)] + [key]


@pytest.fixture
def h(tmp_path, monkeypatch):
    monkeypatch.setattr(cfgc, "HOST_LOG_ENABLED", True, raising=False)
    monkeypatch.setattr(cfgc, "HOST_LOG_NAS_DIR", str(tmp_path / "nas"), raising=False)
    monkeypatch.setattr(cfgc, "HOST_LOG_LOCAL_DIR", str(tmp_path / "local"), raising=False)
    monkeypatch.setattr(cfgc, "HOST_LOG_RETRY_S", 30, raising=False)
    monkeypatch.setattr(cfgc, "HOST_LOG_LOCK_STALE_S", 120, raising=False)
    (tmp_path / "local").mkdir()
    (tmp_path / "nas").mkdir()
    inst = HostProcessLog()
    inst._ensure_worker = lambda: None
    return inst


def _rows(raw: bytes):
    return list(csv.reader(io.StringIO(raw.decode("utf-8-sig"), newline="")))


# ── 경로별 쓰기 함수: (대상 파일, 쓰기, 새 행 바이트) ──
def _pending_path(h):
    p = h._p_pending()
    return p, (lambda: h._append_pending(YMD, _row("k-new"))), _csv_line([YMD] + _row("k-new")).encode()


def _mine_path(h):
    p = h._p_mine(YMD)
    return p, (lambda: h._append_mine(YMD, _row("k-new"))), _csv_line(_row("k-new")).encode()


def _memq_path(h):
    p = h._p_pending()
    ln = _csv_line([YMD] + _row("k-new"))

    def _go():
        h._mem_queue.append(ln)
        h._send_one = lambda ymd, row: False     # NAS 전송은 이 테스트 대상 아님(pending 그대로 둠)
        h._flush_once_locked()
        assert not h._mem_queue
    return p, _go, ln.encode()


def _nas_path(h):
    p = h._p_nas(YMD)
    return p, (lambda: h._send_one(YMD, _row("k-new"))), _csv_line(_row("k-new")).encode()


PATHS = {"pending": _pending_path, "mine": _mine_path, "memq": _memq_path, "nas": _nas_path}
HAS_HEADER = {"pending": False, "mine": True, "memq": False, "nas": True}


@pytest.mark.parametrize("kind", list(PATHS))
def test_cut_file_new_row_on_own_line(h, kind):
    p, write, new = PATHS[kind](h)
    head = (BOM + _csv_line(HEADER).encode()) if HAS_HEADER[kind] else b""
    before = head + CUT
    p.write_bytes(before)
    write()
    raw = p.read_bytes()
    assert raw == before + b"\r\n" + new
    rows = _rows(raw)
    assert rows[-1] == ([YMD] if kind in ("pending", "memq") else []) + _row("k-new")
    assert rows[-2] == ["20261022", "cut-row-partial"]          # 끊긴 행은 그대로 별도 줄
    if kind in ("pending", "memq"):
        # pending 읽기는 짧은 행을 버린다(지금과 같음) → 새 행만 온전히
        assert h._read_pending() == [[YMD] + _row("k-new")]


@pytest.mark.parametrize("kind", list(PATHS))
def test_normal_file_unchanged_plus_row(h, kind):
    p, write, new = PATHS[kind](h)
    head = (BOM + _csv_line(HEADER).encode()) if HAS_HEADER[kind] else b""
    old = _csv_line(([YMD] if kind in ("pending", "memq") else []) + _row("k-old")).encode()
    before = head + old
    p.write_bytes(before)
    write()
    assert p.read_bytes() == before + new


@pytest.mark.parametrize("kind", list(PATHS))
def test_missing_file_same_as_before(h, kind):
    p, write, new = PATHS[kind](h)
    assert not p.exists()
    write()
    head = (BOM + _csv_line(HEADER).encode()) if HAS_HEADER[kind] else b""
    assert p.read_bytes() == head + new


@pytest.mark.parametrize("kind", list(PATHS))
def test_empty_file_same_as_before(h, kind):
    p, write, new = PATHS[kind](h)
    p.write_bytes(b"")
    write()
    assert p.read_bytes() == new                                # 빈 파일은 머리글 없이 이어쓰기(지금과 같음)


def test_nas_already_recorded_key_is_success_and_untouched(h):
    p = h._p_nas(YMD)
    before = BOM + _csv_line(HEADER).encode() + _csv_line(_row("k-dup")).encode() + CUT
    p.write_bytes(before)
    assert h._send_one(YMD, _row("k-dup")) is True
    assert p.read_bytes() == before                              # 이미 기록됨 → 쓰지 않음
    assert not (Path(cfgc.HOST_LOG_NAS_DIR) / "_lock" / "Robot.lock").exists()


def test_helper_failure_does_not_block_append(h, monkeypatch):
    import util.host_process_log as M
    real_open = open

    def _bad_open(path, mode="r", *a, **k):
        if mode == "rb":
            raise PermissionError("읽기 막힘")
        return real_open(path, mode, *a, **k)

    p = h._p_pending()
    p.write_bytes(CUT)
    monkeypatch.setattr(M, "open", _bad_open, raising=False)
    assert h._append_pending(YMD, _row("k-new")) is True        # 보정 실패해도 이어쓰기는 진행
    assert p.read_bytes() == CUT + _csv_line([YMD] + _row("k-new")).encode()

# -*- coding: utf-8 -*-
"""log_sync._copy_one — 복사한 크기로 검증하고, 복사 중 src 가 커지면(쓰는 중) 실패로 세지 않고
copystat 을 생략해 다음 실행에서 다시 복사하는지 (EL-052)."""
from __future__ import annotations

import importlib.util
import os
import shutil

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_spec = importlib.util.spec_from_file_location(
    "log_sync_under_test", os.path.join(ROOT, "apps", "log_sync_service", "log_sync.py"))
LS = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(LS)


@pytest.fixture
def env(tmp_path, monkeypatch):
    src_root, dst_root = tmp_path / "src", tmp_path / "dst"
    src_root.mkdir()
    logs = []
    stats = []
    real_copystat = shutil.copystat

    def _spy_copystat(a, b, *k, **kw):
        stats.append((a, b))
        return real_copystat(a, b, *k, **kw)

    monkeypatch.setattr(LS.shutil, "copystat", _spy_copystat)
    return str(src_root), str(dst_root), logs, stats


def _write(path, data, mtime=1_700_000_000):
    with open(path, "wb") as f:
        f.write(data)
    os.utime(path, (mtime, mtime))


def _run(src_root, dst_root, logs):
    return LS.sync_pair(src_root, dst_root, False, logs.append)


def test_normal_file_copied_then_skipped(env):
    src_root, dst_root, logs, stats = env
    _write(os.path.join(src_root, "a.csv"), b"x,y\r\n1,2\r\n")
    r1 = _run(src_root, dst_root, logs)
    assert (r1["checked"], r1["copied"], r1["skipped"], r1["failed"]) == (1, 1, 0, 0)
    assert len(stats) == 1
    dst = os.path.join(dst_root, "a.csv")
    assert open(dst, "rb").read() == b"x,y\r\n1,2\r\n"
    assert int(os.stat(dst).st_mtime) == 1_700_000_000
    r2 = _run(src_root, dst_root, logs)
    assert (r2["checked"], r2["copied"], r2["skipped"], r2["failed"]) == (1, 0, 1, 0)


def test_growing_file_not_failed_and_recopied(env, monkeypatch):
    src_root, dst_root, logs, stats = env
    src = os.path.join(src_root, "today.csv")
    _write(src, b"head\r\n")
    real_copyfile = shutil.copyfile
    grow = {"on": True}

    def _copy_then_grow(a, b, *k, **kw):
        out = real_copyfile(a, b, *k, **kw)
        if grow["on"]:
            with open(a, "ab") as f:          # 복사 직후 src 에 덧붙임(쓰는 중)
                f.write(b"row\r\n")
        return out

    monkeypatch.setattr(LS.shutil, "copyfile", _copy_then_grow)
    r1 = _run(src_root, dst_root, logs)
    assert (r1["checked"], r1["copied"], r1["skipped"], r1["failed"]) == (1, 1, 0, 0)
    assert stats == []                        # copystat 안 함
    dst = os.path.join(dst_root, "today.csv")
    assert open(dst, "rb").read() == b"head\r\n"
    assert any("쓰는 중 — 다음 실행에서 갱신" in s for s in logs), logs

    grow["on"] = False
    r2 = _run(src_root, dst_root, logs)       # 다음 실행: 다시 복사
    assert (r2["checked"], r2["copied"], r2["skipped"], r2["failed"]) == (1, 1, 0, 0)
    assert open(dst, "rb").read() == b"head\r\nrow\r\n"
    assert len(stats) == 1
    r3 = _run(src_root, dst_root, logs)
    assert (r3["copied"], r3["skipped"], r3["failed"]) == (0, 1, 0)


def test_dst_size_mismatch_is_failure(env, monkeypatch):
    src_root, dst_root, logs, stats = env
    _write(os.path.join(src_root, "b.csv"), b"0123456789")
    real_replace = os.replace

    def _short_replace(a, b):
        real_replace(a, b)
        if str(b).endswith("b.csv"):
            with open(b, "r+b") as f:         # 실제 쓰기 결과가 복사 크기와 다름
                f.truncate(3)

    monkeypatch.setattr(LS.os, "replace", _short_replace)
    r = _run(src_root, dst_root, logs)
    assert (r["checked"], r["copied"], r["skipped"], r["failed"]) == (1, 0, 0, 1)
    assert stats == []
    assert any("크기 불일치 dst=3 복사=10" in s for s in logs), logs
    assert not os.path.exists(os.path.join(dst_root, "b.csv.tmp"))


def test_sync_pair_counts(env, monkeypatch):
    src_root, dst_root, logs, stats = env
    os.makedirs(os.path.join(src_root, "sub"))
    _write(os.path.join(src_root, "same.csv"), b"s")
    _write(os.path.join(src_root, "sub", "new.csv"), b"n")
    _write(os.path.join(src_root, "grow.csv"), b"g")
    _write(os.path.join(src_root, "bad.csv"), b"b")
    os.makedirs(dst_root)
    shutil.copy2(os.path.join(src_root, "same.csv"), os.path.join(dst_root, "same.csv"))
    real_copyfile = shutil.copyfile

    def _cf(a, b, *k, **kw):
        if a.endswith("bad.csv"):
            raise PermissionError("잠김")
        out = real_copyfile(a, b, *k, **kw)
        if a.endswith("grow.csv"):
            with open(a, "ab") as f:
                f.write(b"+")
        return out

    monkeypatch.setattr(LS.shutil, "copyfile", _cf)
    r = _run(src_root, dst_root, logs)
    assert (r["checked"], r["copied"], r["skipped"], r["failed"], r["pair_failed"]) == (4, 2, 1, 1, False)
    assert os.path.exists(os.path.join(dst_root, "sub", "new.csv"))

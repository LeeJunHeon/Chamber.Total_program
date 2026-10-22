# -*- coding: utf-8 -*-
"""log_sync — 제어 파일(이름이 .stop / .flag 로 끝남)은 NAS 로 복사하지 않는다.

예전: OES 폴더(C:\\VanaM_Logs\\OES)를 동기화하면 공정 끝에 메인이 남기는 측정 중지 신호
(.stop_usbN.flag, OES_Data_….csv.stop — 다음 측정 시작 때 지워짐)가 NAS 에 복사돼 쌓였고(log_sync 는 NAS 쪽을
지우지 않는다), 목록을 만든 뒤 지워지면 stat 실패로 집계됐다. 장치·네트워크 접근 없음.
"""
from __future__ import annotations

import importlib.util
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_spec = importlib.util.spec_from_file_location(
    "log_sync_skip_under_test", os.path.join(ROOT, "apps", "log_sync_service", "log_sync.py"))
LS = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(LS)


def _w(p, s="x"):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(s, encoding="utf-8")
    return p


def _files(root):
    return sorted(os.path.relpath(os.path.join(d, f), root) for d, _, fs in os.walk(root) for f in fs)


def test_is_control_file():
    for name in (".stop_usb0.flag", ".STOP_USB1.FLAG", "OES_Data_20261022_100000.csv.stop", "x.Stop"):
        assert LS._is_control_file(name), name
    for name in ("OES_Data_20261022_100000.csv", "stop_report.csv", "flagged.txt", "x.stopped", "a.flags", "CH1.txt"):
        assert not LS._is_control_file(name), name


def test_control_files_not_copied(tmp_path):
    src, dst = tmp_path / "OES", tmp_path / "NAS"
    _w(src / "CH1" / "OES_Data_20261022_100000.csv", "t,a\n1,2\n")
    _w(src / "CH1" / "OES_Data_20261022_100000.csv.stop", "stop\n")
    _w(src / "CH1" / ".stop_usb0.flag", "stop\n")
    _w(src / "CH2" / ".STOP_USB1.FLAG", "stop\n")
    _w(src / "CH2" / "stop_report.csv", "a\n")                  # 이름에 stop 이 들어가도 끝이 다르면 복사
    logs = []
    r = LS.sync_pair(str(src), str(dst), False, logs.append)
    assert (r["checked"], r["copied"], r["skipped"], r["failed"], r["pair_failed"]) == (2, 2, 0, 0, False), (r, logs)
    assert _files(dst) == [os.path.join("CH1", "OES_Data_20261022_100000.csv"), os.path.join("CH2", "stop_report.csv")]
    r2 = LS.sync_pair(str(src), str(dst), False, logs.append)     # 두 번째 실행: 같은 파일은 건너뜀
    assert (r2["checked"], r2["copied"], r2["skipped"], r2["failed"]) == (2, 0, 2, 0)


def test_vanished_control_file_not_counted_as_failure(tmp_path, monkeypatch):
    """목록에 있었는데 복사 전에 지워진 신호 파일 — 예전에는 stat 실패 1건으로 집계."""
    src, dst = tmp_path / "OES", tmp_path / "NAS"
    _w(src / "CH1" / "OES_Data_1.csv", "t,a\n")
    real_walk = os.walk

    def walk(top, *a, **k):
        for d, dirs, files in real_walk(top, *a, **k):
            yield d, dirs, files + ["OES_Data_0.csv.stop", ".stop_usb0.flag"]
    monkeypatch.setattr(LS.os, "walk", walk)
    logs = []
    r = LS.sync_pair(str(src), str(dst), False, logs.append)
    assert (r["checked"], r["copied"], r["failed"]) == (1, 1, 0), (r, logs)
    assert not [m for m in logs if "stat 실패" in m]

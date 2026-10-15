# -*- coding: utf-8 -*-
"""Sputter Calib CSV(Ch1/Ch2_log.csv) — 파일을 기록할 때만 열고 바로 닫는지.
G:(구글 드라이브)는 열려 있는 파일을 닫힐 때까지 올리지 않아, 계속 열어 두면 NAS·다른 PC 에 새 행이 보이지 않는다.
쓰기가 실패하면(엑셀이 잡고 있는 등) 로컬 pending 에 쌓았다가 다음 기록 때 합친다."""
import os
import sys
import csv
import asyncio

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest                                                     # noqa: E402
from controller.data_logger import DataLogger                     # noqa: E402


def _dl(tmp_path, ch=2):
    logs = []
    dl = DataLogger(ch=ch, csv_dir=tmp_path / "nas", log_func=logs.append)
    dl._local_dir = tmp_path / "local"                            # pending 은 시험 폴더에
    return dl, logs


def _rows(path):
    with open(path, encoding="utf-8-sig", newline="") as fp:
        return list(csv.reader(fp))


def _run(dl, name):
    dl.start_new_log_session({"use_ms": False, "process_name": name})
    dl.finalize_and_write_log(True)                               # 루프 밖이면 동기 기록, 안이면 백그라운드 작업


@pytest.mark.parametrize("ch", [1, 2])
def test_not_held_open_at_start_or_after_write(tmp_path, ch):
    dl, logs = _dl(tmp_path, ch)
    _run(dl, "A")                                                  # 파일이 없을 때 — 새로 만들고 닫음
    assert not dl._nas_app.is_open()
    dl2, _ = _dl(tmp_path, ch)                                     # 파일이 있을 때 — 시작하면서 미리 열지 않음
    assert not dl2._nas_app.is_open()
    _run(dl2, "B")
    assert not dl2._nas_app.is_open()
    assert [r[1] for r in _rows(tmp_path / "nas" / f"Ch{ch}_log.csv")[1:]] == ["A", "B"]
    assert any("1행 기록 완료 (NAS)" in m for m in logs)


def test_write_failure_goes_to_pending_then_merges(tmp_path, monkeypatch):
    """본파일 쓰기가 막히면 pending 에 쌓고(바로 닫음), 다음 기록 때 본파일에 합친 뒤 pending 을 지운다."""
    dl, logs = _dl(tmp_path)
    _run(dl, "A")

    def _locked():
        raise PermissionError("다른 프로그램이 파일을 잡고 있음")
    monkeypatch.setattr(dl._nas_app, "open", _locked)
    _run(dl, "B")
    pending = tmp_path / "local" / "Ch2_pending.csv"
    assert pending.exists() and dl._pending_app is None
    assert [r[1] for r in _rows(pending)[1:]] == ["B"]
    monkeypatch.undo()
    _run(dl, "C")
    assert not pending.exists() and not dl._nas_app.is_open()
    assert [r[1] for r in _rows(tmp_path / "nas" / "Ch2_log.csv")[1:]] == ["A", "B", "C"]
    assert any("pending 적재 완료" in m for m in logs) and any("pending → NAS 병합 완료" in m for m in logs)


def test_background_write_closes_file(tmp_path):
    """실제 경로(이벤트 루프 안 → 백그라운드 스레드 기록)에서도 기록 뒤 파일이 닫힌다."""
    async def go():
        dl, _ = _dl(tmp_path)
        _run(dl, "A")
        await asyncio.gather(*(t for t in asyncio.all_tasks() if t is not asyncio.current_task()))
        assert not dl._nas_app.is_open()
        assert [r[1] for r in _rows(tmp_path / "nas" / "Ch2_log.csv")[1:]] == ["A"]
    asyncio.run(go())


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

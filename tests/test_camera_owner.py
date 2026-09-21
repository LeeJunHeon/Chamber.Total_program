# -*- coding: utf-8 -*-
"""CameraRecorder 선점 우선 소유권 검증 — _record_loop 를 대체해 실제 카메라를 열지 않는다.

1) owner 미지정 start/stop 은 기존과 동일 (회귀)
2) chamber1 start 후 chamber2 start → False, 기존 스레드 유지, mode/폴더 CH1 그대로
3) chamber2 의 stop → False, 녹화 계속
4) chamber1 의 stop 만 실제로 정지
5) chamber2 의 set_log_callback → False, 콜백 불변
6) 소유자 해제 후 chamber2 가 정상 start
"""
import os
import sys
import time
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                       # noqa: E402
from util.camera_recorder import CameraRecorder     # noqa: E402


def _fake_loop(self):
    while not self._stop_event.is_set():
        time.sleep(0.01)


@pytest.fixture
def rec(monkeypatch):
    monkeypatch.setattr(CameraRecorder, "_record_loop", _fake_loop)
    monkeypatch.setattr(CameraRecorder, "_load_config", lambda self: None)
    r = CameraRecorder(camera_index=99)
    yield r
    r.stop()
    if r._thread:
        r._thread.join(timeout=1.0)


def _wait_stop(r):
    for _ in range(100):
        if not r.is_running:
            return True
        time.sleep(0.01)
    return False


def test_1_no_owner_legacy(rec):
    assert rec.start("CH1") is True
    assert rec.is_running and rec.current_owner == ""
    t1 = rec._thread
    assert rec.start("CH2") is True                 # owner 없이 재호출 → 기존처럼 재시작
    assert rec._thread is not t1 and rec._mode == "CH2"
    assert rec.set_log_callback(lambda m: None) is True
    assert rec.stop() is True
    assert _wait_stop(rec)


def test_2_second_owner_rejected(rec):
    assert rec.start("CH1", owner="chamber1") is True
    t1 = rec._thread
    folder1 = rec._mode_folder
    assert rec.start("CH2", owner="chamber2") is False
    assert rec._thread is t1 and t1.is_alive()
    assert rec._mode == "CH1" and rec._mode_folder == folder1
    assert rec.current_owner == "chamber1"


def test_3_foreign_stop_ignored(rec):
    rec.start("CH1", owner="chamber1")
    assert rec.stop(owner="chamber2") is False
    assert rec.is_running and rec.current_owner == "chamber1"


def test_4_owner_stop_works(rec):
    rec.start("CH1", owner="chamber1")
    rec.stop(owner="chamber2")
    assert rec.stop(owner="chamber1") is True
    assert _wait_stop(rec)
    assert rec.current_owner == ""


def test_5_foreign_log_callback_ignored(rec):
    def cb1(m):
        pass

    def cb2(m):
        pass
    rec.start("CH1", owner="chamber1")
    assert rec.set_log_callback(cb1, owner="chamber1") is True
    assert rec.set_log_callback(cb2, owner="chamber2") is False
    assert rec._log_cb is cb1
    assert rec.set_log_callback(cb2) is True          # owner 미지정은 기존대로 덮어씀
    assert rec._log_cb is cb2


def test_6_after_release_other_owner_can_start(rec):
    rec.start("CH1", owner="chamber1")
    rec.stop(owner="chamber1")
    assert _wait_stop(rec)
    assert rec.start("CH2", owner="chamber2") is True
    assert rec.is_running and rec._mode == "CH2" and rec.current_owner == "chamber2"
    # 같은 owner 재호출은 멱등(재시작) + True
    t = rec._thread
    assert rec.start("CH2", owner="chamber2") is True
    assert rec._thread is not t and rec.is_running


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

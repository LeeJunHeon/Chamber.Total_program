# -*- coding: utf-8 -*-
"""CameraRecorder 참여자 집합 + ALL 승격(강등 없음) 검증 — _record_loop 를 대체해 실제 카메라를 열지 않는다.

1) owner 미지정 start/stop 은 기존과 동일 (회귀)
2) chamber1 start → current_mode == "CH1", 스레드 1개
3) 같은 chamber1 재 start → 스레드 객체 동일, 재시작 없음 (★ 현재 버그 회귀)
4) chamber1 촬영 중 chamber2 start → "ALL" 승격, users 2개
5) chamber2 stop → False, 촬영 계속, "ALL" 유지(강등 없음)
6) chamber1 stop → True, 실제 정지
7) chamber1 + chamber2 각각 set_log_callback → _log 시 두 콜백 모두 호출
8) pc1 단독 start → "CLEANING"
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
    assert rec.is_running and rec.current_mode == "CH1" and rec.current_users == set()
    t1 = rec._thread
    assert rec.start("CH2") is True                 # owner 없이 재호출 → 기존처럼 재시작
    assert rec._thread is not t1 and rec.current_mode == "CH2"
    rec.set_log_callback(lambda m: None)
    assert rec._log_cb is not None
    assert rec.stop() is True
    assert _wait_stop(rec)


def test_2_single_owner_mode(rec):
    assert rec.start(owner="chamber1") is True
    assert rec.current_mode == "CH1" and rec._mode_folder == "CH1"
    assert rec.current_users == {"chamber1"}
    assert rec._thread.is_alive()


def test_3_same_owner_no_restart(rec):
    rec.start(owner="chamber1")
    t1 = rec._thread
    assert rec.start(owner="chamber1") is True
    assert rec._thread is t1 and t1.is_alive() and rec.current_mode == "CH1"
    # mode 인자를 넘겨도 무시된다(참여자 집합이 모드를 정한다)
    assert rec.start("CH2", owner="chamber1") is True
    assert rec._thread is t1 and rec.current_mode == "CH1"


def test_4_second_owner_promotes_to_all(rec):
    rec.start(owner="chamber1")
    t1 = rec._thread
    msgs = []
    rec.set_log_callback(msgs.append)
    assert rec.start(owner="chamber2") is True
    assert rec.current_mode == "ALL" and rec._mode_folder == "ALL"
    assert rec.current_users == {"chamber1", "chamber2"}
    assert rec._thread is not t1 and rec.is_running
    assert any("카메라 모드 승격 CH1→ALL" in m for m in msgs), msgs
    # 승격 후 같은 참여자 재요청은 재시작 없음
    t2 = rec._thread
    rec.start(owner="chamber1")
    assert rec._thread is t2


def test_5_partial_stop_keeps_all(rec):
    rec.start(owner="chamber1")
    rec.start(owner="chamber2")
    t = rec._thread
    assert rec.stop(owner="chamber2") is False
    assert rec.is_running and rec._thread is t
    assert rec.current_mode == "ALL" and rec.current_users == {"chamber1"}
    # 강등 없음: chamber1 이 다시 start 해도 ALL 유지, 재시작 없음
    rec.start(owner="chamber1")
    assert rec.current_mode == "ALL" and rec._thread is t


def test_6_last_owner_stop_stops(rec):
    rec.start(owner="chamber1")
    rec.start(owner="chamber2")
    rec.stop(owner="chamber2")
    assert rec.stop(owner="chamber1") is True
    assert _wait_stop(rec)
    assert rec.current_users == set()
    # 다시 단독 시작하면 그 참여자 모드로 새 세션
    assert rec.start(owner="chamber2") is True
    assert rec.current_mode == "CH2"


def test_7_per_owner_log_callbacks(rec):
    got1, got2 = [], []
    rec.set_log_callback(got1.append, owner="chamber1")
    rec.set_log_callback(lambda m: (_ for _ in ()).throw(RuntimeError("boom")), owner="pcX")   # 하나 실패해도 전파 안 됨
    rec.set_log_callback(got2.append, owner="chamber2")
    rec._log("hello")
    assert got1 == ["hello"] and got2 == ["hello"]
    # 모두 정지하면 owner 콜백은 정리된다
    rec.start(owner="chamber1")
    rec.stop(owner="chamber1")
    assert rec._log_cbs == {}


def test_8_pc_alone_is_cleaning(rec):
    assert rec.start(owner="pc1") is True
    assert rec.current_mode == "CLEANING" and rec._mode_folder == "CLEANING"
    rec.stop(owner="pc1")
    assert _wait_stop(rec)
    assert rec.start(owner="pc2") is True
    assert rec.current_mode == "CLEANING"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

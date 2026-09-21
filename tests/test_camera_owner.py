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


# ── PC 런타임의 owner 고정 (공정 중 채널 전환) ─────────────────────
def _pc_with(rec):
    """PlasmaCleaningRuntime 을 __init__ 없이 최소 구성 (카메라 호출부만 사용)."""
    from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime
    p = PlasmaCleaningRuntime.__new__(PlasmaCleaningRuntime)
    p._selected_ch = 1
    p._cam_owner_active = ""
    p.camera_recorder = rec
    p.logs = []
    p.append_log = lambda src, msg: p.logs.append(f"[{src}] {msg}")
    p._cam_log = lambda msg: p.append_log("CAM", msg)
    return p


def _pc_cam_start(p):
    rec = p.camera_recorder
    owner = p._cam_owner_for_start()
    rec.set_log_callback(p._cam_log, owner=owner)
    rec.start(owner=owner)


def _pc_cam_stop(p):
    rec = p.camera_recorder
    ok = rec.stop(owner=p._cam_owner_for_stop())
    p._cam_owner_active = ""
    return ok


def test_pc_channel_switch_mid_run(rec):
    p = _pc_with(rec)
    _pc_cam_start(p)
    assert rec.current_users == {"pc1"} and rec.current_mode == "CLEANING"
    p._selected_ch = 2                         # 공정 중 라디오 전환
    assert _pc_cam_stop(p) is True
    assert rec.current_users == set()
    assert _wait_stop(rec)
    assert p._cam_owner_active == ""


def test_pc_owner_cleared_after_stop(rec):
    rec.start(owner="chamber1")                # 다른 공정이 촬영 중
    p = _pc_with(rec)
    _pc_cam_start(p)
    assert rec.current_mode == "ALL" and rec.current_users == {"chamber1", "pc1"}
    p._selected_ch = 2
    assert _pc_cam_stop(p) is False            # chamber1 이 남아 촬영 계속
    assert rec.is_running and rec.current_users == {"chamber1"}
    assert p._cam_owner_active == ""
    # 시작·승격 메시지는 콜백을 먼저 등록했으므로 PC 공정 로그에 남는다
    assert any("카메라 모드 승격 CH1→ALL" in m for m in p.logs), p.logs


# ── 로그 콜백 락 분리 회귀 (start() 의 join 이 촬영 스레드의 finally _log 와 교착하던 문제) ──
def _loop_with_final_log(self):
    try:
        while not self._stop_event.is_set():
            time.sleep(0.02)
    finally:
        self._log("완료 — 촬영 0장 | 폴더: (stub)")


@pytest.fixture
def rec_final(monkeypatch):
    monkeypatch.setattr(CameraRecorder, "_record_loop", _loop_with_final_log)
    monkeypatch.setattr(CameraRecorder, "_load_config", lambda self: None)
    r = CameraRecorder(camera_index=99)
    yield r
    r.stop()
    if r._thread:
        r._thread.join(timeout=1.0)


def test_promote_restart_is_not_blocked_by_log_lock(rec_final):
    r = rec_final
    r.start(owner="chamber1")
    time.sleep(0.2)
    t0 = time.perf_counter()
    r.start(owner="chamber2")
    dt = time.perf_counter() - t0
    assert dt < 1.0, f"승격 재시작이 {dt:.3f}s 걸렸다 (join 이 _log 락에 막힘)"
    assert r.current_mode == "ALL" and r.current_users == {"chamber1", "chamber2"}
    r.stop(owner="chamber1"); r.stop(owner="chamber2")
    assert _wait_stop(r)


def test_log_fanout_during_restart(rec_final):
    r = rec_final
    got1, got2 = [], []
    r.set_log_callback(got1.append, owner="chamber1")
    r.set_log_callback(got2.append, owner="chamber2")
    r.start(owner="chamber1")
    time.sleep(0.1)
    r.start(owner="chamber2")
    assert any("카메라 모드 승격" in m for m in got1), got1
    assert any("카메라 모드 승격" in m for m in got2), got2
    r.stop()
    assert _wait_stop(r)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
